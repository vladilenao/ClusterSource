"""Архивация: перенос завершённых дней горячего хранилища в Parquet и очистка.

Запускается при старте и ежесуточно в ``archiver.run_hour_utc:run_minute_utc``
(по умолчанию 00:05 UTC) в составе ночного обслуживания (см. ``main``).
Репозиторий каждого инструмента обрабатывается независимо:

1. Найти в SQLite все прошедшие дни (раньше сегодняшнего), у которых ещё нет
   Parquet-файла.
2. Записать каждый в ``data/history/{storage}/{YYYY-MM-DD}.parquet`` (snappy)
   и проверить, что файл существует и не пуст.
3. Очистить строки горячего хранилища старше окна удержания (7 дней) — только
   если у каждого удаляемого дня есть Parquet-файл.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime, timedelta

from cluster_source.config import AppConfig, ArchiverConfig
from cluster_source.database import (
    HOT_RETENTION_DAYS,
    DataCorruptionError,
    MarketDataRepository,
)

logger = logging.getLogger(__name__)


def _utc_today() -> datetime:
    return datetime.now(UTC).replace(hour=0, minute=0, second=0, microsecond=0)


def yesterday() -> datetime:
    return _utc_today() - timedelta(days=1)


def _hot_purge_cutoff() -> datetime:
    """Граница по целому дню, совпадающая с ``autoload._target_for_day``.

    Очищает полные дни строго старше окна удержания в 7 *календарных* дней
    (полночь сегодня−7). Если использовать ``now − 7d``, то пограничный день,
    который догрузка только что направила в SQLite, был бы вычищен до того,
    как его заархивируют, и молча потерялся.
    """
    return _utc_today() - timedelta(days=HOT_RETENTION_DAYS)


def seconds_until_next_run(now: datetime, schedule: ArchiverConfig) -> float:
    """Секунды от ``now`` до ближайшего ``run_hour_utc:run_minute_utc`` UTC (строго в будущем)."""
    now = now.astimezone(UTC)
    target = now.replace(
        hour=schedule.run_hour_utc, minute=schedule.run_minute_utc, second=0, microsecond=0
    )
    if target <= now:
        target += timedelta(days=1)
    return (target - now).total_seconds()


class DataArchiver:
    """Архивирует горячее хранилище одного инструмента в холодное Parquet."""

    def __init__(self, config: AppConfig, repo: MarketDataRepository) -> None:
        self._config = config
        self._repo = repo

    async def archive_day(self, day: datetime) -> bool:
        """Записывает ``day`` (UTC) из горячего SQLite в Parquet и проверяет файл.

        Возвращает True, если день заархивирован (или архивировать было
        нечего), и False, если запись не прошла проверку. SQLite не изменяется.
        """
        day = day.replace(hour=0, minute=0, second=0, microsecond=0)
        day_str = day.strftime("%Y-%m-%d")
        storage = self._repo.instrument.storage_name

        df = await asyncio.to_thread(self._repo.hot_rows_for_day, day)
        if df.empty:
            logger.info(
                "Нет горячих тиков для %s за %s — пропускаем архивацию",
                storage,
                day_str,
            )
            return True

        try:
            path = await asyncio.to_thread(self._repo.write_parquet, day_str, df)
        except DataCorruptionError as exc:
            logger.error(
                "АРХИВАЦИЯ НЕ УДАЛАСЬ для %s %s — горячие строки НЕ удалены: %s",
                storage,
                day_str,
                exc,
            )
            return False
        logger.info("Архивировано %s %s -> %s (%d строк)", storage, day_str, path, len(df))
        return True

    async def run_once(self) -> None:
        """Архивирует все незаархивированные прошедшие дни и чистит устаревшие горячие данные."""
        storage = self._repo.instrument.storage_name
        today_str = _utc_today().strftime("%Y-%m-%d")
        hot_days = await asyncio.to_thread(self._repo.hot_days_available)
        cold_days = await asyncio.to_thread(self._repo.parquet_days_available)
        for day_str in sorted(d for d in hot_days - cold_days if d < today_str):
            await self.archive_day(datetime.fromisoformat(day_str).replace(tzinfo=UTC))

        # Удаляем строки только тех дней, которые гарантированно лежат в Parquet.
        cutoff = _hot_purge_cutoff()
        cutoff_str = cutoff.strftime("%Y-%m-%d")
        cold_days = await asyncio.to_thread(self._repo.parquet_days_available)
        unarchived = sorted(d for d in hot_days if d < cutoff_str and d not in cold_days)
        if unarchived:
            logger.error(
                "Очистка горячего хранилища %s отменена: нет Parquet для дней %s",
                storage,
                ", ".join(unarchived),
            )
            return
        purged = await asyncio.to_thread(self._repo.purge_hot_before, cutoff)
        logger.info("Удалено %d горячих строк старше %s для %s", purged, cutoff, storage)


async def run_archiver(config: AppConfig, repos: dict[str, MarketDataRepository]) -> None:
    """Один раз архивирует все инструменты (при старте и в ночном обслуживании)."""
    archivers = [DataArchiver(config, repo) for repo in repos.values()]
    await asyncio.gather(*(a.run_once() for a in archivers))
