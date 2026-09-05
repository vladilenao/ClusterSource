"""Ночная архивация: перенос вчерашних тиков горячего хранилища в Parquet и очистка.

Запускается в запланированное время UTC (по умолчанию 00:05). Репозиторий
каждого инструмента обрабатывается независимо:

1. Выбрать все тики за вчера (00:00:00–23:59:59 UTC) из SQLite.
2. Записать их в ``data/history/{storage}/{YYYY-MM-DD}.parquet`` (snappy).
3. Проверить, что файл существует и не пуст, *до* удаления чего-либо из SQLite.
4. Очистить строки горячего хранилища старше окна удержания (7 дней).
"""

from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime, timedelta

from cluster_source.config import AppConfig
from cluster_source.database import (
    HOT_RETENTION_DAYS,
    DataCorruptionError,
    MarketDataRepository,
)

logger = logging.getLogger(__name__)


def yesterday() -> datetime:
    now = datetime.now(UTC)
    return (now - timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)


def _hot_purge_cutoff() -> datetime:
    """Граница по целому дню, совпадающая с ``autoload._target_for_day``.

    Очищает полные дни строго старше окна удержания в 7 *календарных* дней
    (полночь сегодня−7). Если использовать ``now − 7d``, то пограничный день,
    который догрузка только что направила в SQLite, был бы вычищен до того,
    как его заархивируют, и молча потерялся.
    """
    now = datetime.now(UTC)
    return now.replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(
        days=HOT_RETENTION_DAYS
    )


class DataArchiver:
    """Архивирует горячее хранилище одного инструмента в холодное Parquet."""

    def __init__(self, config: AppConfig, repo: MarketDataRepository) -> None:
        self._config = config
        self._repo = repo

    async def archive_day(self, day: datetime) -> bool:
        """Архивирует ``day`` (UTC) из горячего SQLite в Parquet.

        Возвращает True, если день заархивирован (или архивировать было
        нечего), и False, если день пропущен из-за порчи данных. Очистка
        выполняется только после успешной записи.
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
            logger.info("Архивировано %s %s -> %s (%d строк)", storage, day_str, path, len(df))
        except DataCorruptionError as exc:
            logger.error(
                "АРХИВАЦИЯ НЕ УДАЛАСЬ для %s %s — горячие строки НЕ удалены: %s",
                storage,
                day_str,
                exc,
            )
            return False

        # Проверка пройдена (write_parquet выбрасывает ошибку при некорректном
        # выводе). Теперь безопасно очищать горячее хранилище за окном удержания.
        cutoff = _hot_purge_cutoff()
        purged = await asyncio.to_thread(self._repo.purge_hot_before, cutoff)
        logger.info("Удалено %d горячих строк старше %s для %s", purged, cutoff, storage)
        return True

    async def run_once(self) -> None:
        """Архивирует вчерашний день и чистит устаревшие горячие данные."""
        await self.archive_day(yesterday())


async def run_archiver(config: AppConfig, repos: dict[str, MarketDataRepository]) -> None:
    """Один раз архивирует все инструменты (по расписанию и при старте)."""
    archivers = [DataArchiver(config, repo) for repo in repos.values()]
    await asyncio.gather(*(a.run_once() for a in archivers))
