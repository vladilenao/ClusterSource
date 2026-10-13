"""Проверка полноты истории и автоматическая догрузка.

Выполняется при старте и в ночном обслуживании. Для каждого инструмента
обрабатываются дни за последние ``history_check_days``:

* отсутствующие — ни одной строки ни в SQLite, ни в Parquet;
* отмеченные для перекачки (``MarketDataRepository.days_to_refresh``):
  разрыв ``trade_id`` (``compromised``), сделки до открытия/переоткрытия стрима
  не получены (``stream_gap``), сегодняшний день догружен лишь частично
  (``partial_backfill``). Отмеченный день заменяется данными официального
  архива целиком, как только архив за этот день опубликован.

Сохраняются только биржевые сделки (SPEC-0002): строки архива с
``TRADE_SOURCE`` ≠ ``EXCHANGE`` и дилерские сделки ``GetLastTrades``
отбрасываются. Однократная чистка ранее сохранённых данных — отметки
``dealer_cleanup`` (:func:`schedule_dealer_cleanup`); они не ограничены окном
``history_check_days`` и обрабатываются отдельным проходом ``cleanup=True``.

Основной источник истории: официальный сервис истории сделок, один файл на
инструмент и календарный день с анонимизированными сделками:
``GET https://invest-public-api.tbank.ru/history-trades/YYYY-MM-DD?instrumentId={TICKER_CLASS}``
(аутентификация Bearer). Ответ — gzip'нутый CSV (заголовок
``TRADE_TS,TICKER_CC,DIRECTION,PRICE,QUANTITY,TRADE_SOURCE,INSTRUMENT_UID``),
архивы обновляются каждую ночь и покрывают все прошедшие торговые дни.
404 = в этот день не было торгов (или архив ещё не опубликован). Шлюз
ограничивает загрузку ~30 файлами в минуту на IP.

Для текущего дня архив ещё не опубликован, поэтому догрузка откатывается на
аутентифицированный ``MarketDataService/GetLastTrades`` (только последние
сделки) — сначала через SDK-шный gRPC, когда он доступен, затем через REST.
Такой день отмечается ``partial_backfill`` и перекачивается из архива позже.

Резолюция FIGI / контракта предпочитает SDK ``InstrumentsService`` (REST-шлюз
``FindInstrument`` периодически отдаёт 404); REST оставлен только как фолбэк
для окружений без SDK.

Легаси-архив публичных сделок (``https://tbank.ru/invest-api/data-api/...``)
прекратил работу (HTTP 404); возобновлять его НЕЛЬЗЯ.
"""

from __future__ import annotations

import asyncio
import contextlib
import csv
import gzip
import io
import logging
import ssl
from collections.abc import Iterable
from datetime import UTC, date, datetime, timedelta
from enum import StrEnum
from typing import Any, cast

import aiohttp
import pandas as pd

from cluster_source.config import (
    AppConfig,
    FuturesMeta,
    Instrument,
    InstrumentType,
    is_exchange_trade,
    nearest_contract,
    parse_direction,
)
from cluster_source.database import HOT_RETENTION_DAYS, MarketDataRepository

logger = logging.getLogger(__name__)

REST_BASE = "https://invest-public-api.tinkoff.ru/rest/tinkoff.public.invest.api.contract.v1"
LAST_TRADES_URL = f"{REST_BASE}/MarketDataService/GetLastTrades"
FIND_INSTRUMENT_URL = f"{REST_BASE}/InstrumentsService/FindInstrument"
FUTURES_URL = f"{REST_BASE}/InstrumentsService/Futures"
HISTORY_TRADES_URL = "https://invest-public-api.tbank.ru/history-trades"
MAX_BACKFILL_CONCURRENCY = 4

#: Шлюз архива истории ограничивает загрузку ~30 файлами в минуту на IP.
#: Держим темп чуть ниже (28/мин), чтобы не упираться в 429.
ARCHIVE_MAX_PER_MIN = 28
ARCHIVE_ATTEMPTS = 3

#: Причина отметки сегодняшнего дня, догруженного через GetLastTrades.
REFRESH_PARTIAL_BACKFILL = "partial_backfill"

#: SPEC-0002 G-4: однократная перекачка всех сохранённых дней без дилерских сделок.
REFRESH_DEALER_CLEANUP = "dealer_cleanup"
#: SPEC-0006: день не совпал с эталоном биржи — перекачать из архива независимо от
#: давности (окно ``history_check_days`` на эту отметку не действует).
REFRESH_FINAL_MISMATCH = "final_mismatch"
DEALER_CLEANUP_META_KEY = "spec0002_dealer_cleanup_scheduled"
#: Архив за день гарантированно опубликован через столько суток: 404 по отметке
#: чистки тогда означает, что биржевых сделок за день не было.
ARCHIVE_PUBLISHED_AFTER_DAYS = 2

#: Идентификаторы «тикер/класс», которые принимает архив истории для каждого
#: типа инструмента. Используются как фолбэк, когда код класса API не совпадает
#: (для акций конвенция архива отличается от ``classCode`` в find_instrument).
ARCHIVE_CLASS_CANDIDATES: dict[InstrumentType, list[str]] = {
    InstrumentType.SHARE: ["TQBR", "SPBXM"],
    InstrumentType.FUTURES_CONTRACT: ["SPBFUT"],
    InstrumentType.CONTINUOUS_FUTURES: ["SPBFUT"],
}

#: Приоритет классов при выборе акции среди одноимённых тикеров разных площадок
#: (например, "T" -> AT&T и TQBR "Т-Технологии").
SHARE_CLASS_PRIORITY = ("TQBR", "SPBXM")


class BackfillResult(StrEnum):
    """Итог догрузки одного дня."""

    WRITTEN = "written"  # данные получены и записаны
    NO_DATA = "no_data"  # источник ответил, но сделок за день нет (404 / пусто)
    #: архив опубликован, но все его сделки небиржевые (SPEC-0002)
    NO_EXCHANGE_TRADES = "no_exchange_trades"
    FAILED = "failed"  # ошибка; день будет обработан при следующем запуске


class _RateGate:
    """Глобальный ограничитель темпа: не чаще одного прохода в ``interval``.

    Шлюз ``history-trades`` (~30 файлов/мин на IP) не переживает всплесков:
    при честном неделимом интервале между запросами он успевает батчить
    по 2 секунды, и число запросов в минуту гарантированно не превысит
    60 / interval. Сериализует ВСЕ конкурентные backfill-задачи.
    """

    def __init__(self, interval: float) -> None:
        self._interval = interval
        self._lock = asyncio.Lock()
        self._last = 0.0

    async def wait(self) -> None:
        async with self._lock:
            now = asyncio.get_running_loop().time()
            delay = self._last + self._interval - now
            if delay > 0:
                await asyncio.sleep(delay)
            self._last = asyncio.get_running_loop().time()


def archive_gate() -> _RateGate:
    """Ограничитель темпа архива истории; один на процесс, общий для всех проходов."""
    return _RateGate(60.0 / ARCHIVE_MAX_PER_MIN)


try:
    import truststore as _truststore_mod

    _truststore: Any = _truststore_mod
except ImportError:
    _truststore = None

_injected_truststore = False


def _ssl_context() -> ssl.SSLContext:
    """TLS-контекст, доверяющий системному хранилищу сертификатов.

    В macOS-сборке интерпретатора python.org нет файла доверенных CA по
    умолчанию, поэтому простой ``ssl.create_default_context()`` падает
    («unable to get local issuer certificate»). Эндпоинты T-Investments также
    подписаны российским Trusted Root CA, который есть в связке ключей macOS,
    но отсутствует в pip-бандле CA — поэтому используем *системное*
    хранилище доверия (``truststore``) с фолбэком на бандл certifi.
    Проверка остаётся полностью включённой.
    """
    global _injected_truststore
    if _truststore is not None:
        if not _injected_truststore:
            _truststore.inject_into_ssl()
            _injected_truststore = True
        return ssl.create_default_context()

    import certifi

    return ssl.create_default_context(cafile=certifi.where())


def missing_days(config: AppConfig, repo: MarketDataRepository) -> list[datetime]:
    """Возвращает UTC-даты (полночь) за последние N дней без каких-либо данных."""
    today = _utc_today()
    available = repo.available_days()
    missing: list[datetime] = []
    for offset in range(config.history_check_days):
        day = today - timedelta(days=offset)
        day_str = day.strftime("%Y-%m-%d")
        if day_str not in available:
            missing.append(day)
    return missing


def refresh_days(
    config: AppConfig, repo: MarketDataRepository, *, cleanup: bool = False
) -> list[datetime]:
    """Отмеченные для перекачки прошедшие дни.

    Сегодняшний день не возвращается: его архив ещё не опубликован.

    * ``cleanup=False`` — все отметки, кроме ``dealer_cleanup``, внутри окна
      ``history_check_days``; отметки старше окна снимаются с предупреждением,
      кроме ``final_mismatch`` (SPEC-0006) — она окном не ограничена.
    * ``cleanup=True`` — только отметки ``dealer_cleanup`` без ограничения
      окном (SPEC-0002 G-4), от новых дней к старым.
    """
    today = _utc_today()
    window_start = (today - timedelta(days=config.history_check_days - 1)).strftime("%Y-%m-%d")
    today_str = today.strftime("%Y-%m-%d")
    days: list[datetime] = []
    for day_str, reason in sorted(repo.days_to_refresh().items(), reverse=cleanup):
        is_cleanup = reason == REFRESH_DEALER_CLEANUP
        if is_cleanup != cleanup or day_str >= today_str:
            continue
        if not cleanup and day_str < window_start and reason != REFRESH_FINAL_MISMATCH:
            logger.warning(
                "Отметка перекачки %s за %s (%s) вышла за окно %d дней — снята без перекачки",
                repo.instrument.storage_name,
                day_str,
                reason,
                config.history_check_days,
            )
            repo.clear_refresh_mark(day_str)
            continue
        days.append(datetime.fromisoformat(day_str).replace(tzinfo=UTC))
    return days


def schedule_dealer_cleanup(repo: MarketDataRepository) -> int:
    """Однократно отмечает все сохранённые прошедшие дни для перекачки без дилерских сделок.

    SPEC-0002 G-4: до введения фильтра в хранилище попадали дилерские сделки.
    Сегодняшний день не отмечается — коллектор уже отметил его ``stream_gap``
    при открытии стрима, и ночное обслуживание перекачает его из архива.
    Повторный вызов ничего не делает (флаг в ``meta``). Возвращает число
    отмеченных дней.
    """
    if repo.get_meta(DEALER_CLEANUP_META_KEY) is not None:
        return 0
    today_str = _utc_today().strftime("%Y-%m-%d")
    days = sorted(d for d in repo.available_days() if d < today_str)
    repo.mark_days_for_refresh(days, REFRESH_DEALER_CLEANUP, override=True)
    repo.set_meta(DEALER_CLEANUP_META_KEY, datetime.now(UTC).isoformat())
    logger.info(
        "Чистка от дилерских сделок: %s — отмечено %d дней для перекачки",
        repo.instrument.storage_name,
        len(days),
    )
    return len(days)


def _utc_today() -> datetime:
    return datetime.now(UTC).replace(hour=0, minute=0, second=0, microsecond=0)


def _parse_history_csv(content: bytes) -> pd.DataFrame:
    """Разбирает CSV, который выдаёт эндпоинт архива истории T-Investments."""
    text = content.decode("utf-8-sig", errors="replace")
    rows = list(csv.reader(io.StringIO(text)))
    if not rows:
        return pd.DataFrame()
    header = [h.strip().lower() for h in rows[0]]
    data = rows[1:]

    def col(name: str) -> int:
        for i, h in enumerate(header):
            if name in h:
                return i
        return -1

    i_ts = col("trade_ts")
    if i_ts < 0:
        i_ts = col("time")
    if i_ts < 0:
        i_ts = col("timestamp")
    i_price = col("price")
    i_vol = col("quantity") if col("quantity") >= 0 else col("volume")
    i_dir = col("direction") if col("direction") >= 0 else -1
    i_tid = col("tradeid") if col("tradeid") >= 0 else col("trade_id")
    i_src = col("trade_source")

    parsed: list[dict[str, object]] = []
    for row in data:
        try:
            parsed.append(
                {
                    "timestamp": row[i_ts] if 0 <= i_ts < len(row) else "",
                    "price": float(row[i_price]) if 0 <= i_price < len(row) else 0.0,
                    "volume": int(row[i_vol]) if 0 <= i_vol < len(row) else 0,
                    "direction": row[i_dir] if 0 <= i_dir < len(row) else "",
                    "trade_id": row[i_tid] if 0 <= i_tid < len(row) else "",
                    "trade_source": row[i_src] if 0 <= i_src < len(row) else "",
                }
            )
        except (ValueError, IndexError):
            continue
    df = pd.DataFrame(parsed)
    if not df.empty and "timestamp" in df.columns:
        # format="ISO8601": формат определяется для каждой строки, иначе строки
        # без дробной части секунд превратились бы в NaT и молча отбросились.
        df["timestamp"] = pd.to_datetime(
            df["timestamp"], utc=True, errors="coerce", format="ISO8601"
        )
        df = df.dropna(subset=["timestamp"])
    return df


def _target_for_day(day: datetime) -> str:
    if day >= _utc_today() - timedelta(days=HOT_RETENTION_DAYS):
        return "sqlite"
    return "parquet"


async def backfill_day(
    session: aiohttp.ClientSession,
    config: AppConfig,
    instrument: Instrument,
    repo: MarketDataRepository,
    day: datetime,
    figi: str | None = None,
    archive_keys: list[str] | None = None,
    market_data: Any | None = None,
    gate: _RateGate | None = None,
    replace: bool = False,
) -> BackfillResult:
    """Догружает один день.

    Прошедшие дни берутся из официального архива истории (``history-trades``)
    по кандидатным ключам тикер/класс; текущий день — из ``GetLastTrades``,
    потому что архив публикуется только на следующую ночь. ``replace=True``
    заменяет уже сохранённые данные дня (перекачка отмеченного дня).
    """
    day_str = day.strftime("%Y-%m-%d")
    if day.date() >= datetime.now(UTC).date():
        if figi is None:
            logger.error(
                "Невозможно догрузить %s %s: не удалось разрешить FIGI",
                instrument.id,
                day_str,
            )
            return BackfillResult.FAILED
        return await _backfill_current_session(
            session, config, instrument, repo, day, figi, market_data
        )
    if not archive_keys:
        logger.error(
            "Невозможно догрузить %s %s: ключ архива истории недоступен",
            instrument.id,
            day_str,
        )
        return BackfillResult.FAILED
    return await _backfill_from_archive(
        session, config, instrument, repo, day, archive_keys, gate, replace
    )


async def _backfill_from_archive(
    session: aiohttp.ClientSession,
    config: AppConfig,
    instrument: Instrument,
    repo: MarketDataRepository,
    day: datetime,
    archive_keys: list[str],
    gate: _RateGate | None = None,
    replace: bool = False,
) -> BackfillResult:
    """Скачивает официальный дневной архив сделок (gzip-CSV) и сохраняет его."""
    if gate is None:
        gate = _RateGate(60.0 / ARCHIVE_MAX_PER_MIN)
    day_str = day.strftime("%Y-%m-%d")
    headers = {"Authorization": f"Bearer {config.token}", "accept": "application/octet-stream"}

    content: bytes | None = None
    exhausted = False
    for key in archive_keys:
        url = f"{HISTORY_TRADES_URL}/{day_str}?instrumentId={key}"
        logger.info("Загрузка архива истории %s для %s (%s)", key, instrument.id, day_str)
        for attempt in range(ARCHIVE_ATTEMPTS):
            await gate.wait()
            try:
                async with session.get(
                    url, headers=headers, timeout=aiohttp.ClientTimeout(total=120)
                ) as resp:
                    if resp.status == 404:
                        break
                    if resp.status == 429:  # ~30 загрузок в минуту на IP
                        retry_after = float(resp.headers.get("Retry-After", 60) or 60)
                        logger.warning(
                            "Архив истории ограничил лимит для %s за %s: "
                            "повтор через %.0f c (попытка %d/%d)",
                            instrument.id,
                            day_str,
                            retry_after,
                            attempt + 1,
                            ARCHIVE_ATTEMPTS,
                        )
                        if attempt + 1 >= ARCHIVE_ATTEMPTS:
                            exhausted = True
                            break
                        await asyncio.sleep(retry_after)
                        continue
                    resp.raise_for_status()
                    content = await resp.read()
                    break
            except aiohttp.ClientError as exc:
                logger.error("Ошибка загрузки архива истории для %s: %s", instrument.id, exc)
                return BackfillResult.FAILED
        if content is not None:
            break
    if content is None:
        if exhausted:
            logger.warning(
                "Архив истории недоступен для %s за %s: лимит запросов исчерпан",
                instrument.id,
                day_str,
            )
            return BackfillResult.FAILED
        logger.info(
            "Архива истории нет для %s за %s (в этот день не было торгов)",
            instrument.id,
            day_str,
        )
        return BackfillResult.NO_DATA

    if content[:2] == b"\x1f\x8b":
        try:
            content = gzip.decompress(content)
        except OSError as exc:
            logger.error("Повреждён архив истории для %s: %s", instrument.id, exc)
            return BackfillResult.FAILED

    df = _parse_history_csv(content)
    if df.empty:
        logger.info("Нет сделок в архиве для %s за %s", instrument.id, day_str)
        return BackfillResult.NO_DATA
    df, dropped = _exchange_only(df)
    if dropped:
        logger.info(
            "Архив %s за %s: отброшено %d небиржевых сделок", instrument.id, day_str, dropped
        )
    if df.empty:
        logger.info("В архиве %s за %s нет биржевых сделок", instrument.id, day_str)
        return BackfillResult.NO_EXCHANGE_TRADES

    rows = _history_rows(df, instrument)
    rows = [r for r in rows if _as_utc(r["timestamp"]).strftime("%Y-%m-%d") == day_str]
    if not rows:
        logger.info("Нет сделок в окне для %s за %s", instrument.id, day_str)
        return BackfillResult.NO_DATA
    await _persist_day(repo, day, rows, replace=replace)
    return BackfillResult.WRITTEN


async def _backfill_current_session(
    session: aiohttp.ClientSession,
    config: AppConfig,
    instrument: Instrument,
    repo: MarketDataRepository,
    day: datetime,
    figi: str,
    market_data: Any | None,
) -> BackfillResult:
    """Получает последние сделки (GetLastTrades), сохраняет их и отмечает день неполным."""
    day_str = day.strftime("%Y-%m-%d")
    raw_trades: list[Any] = []
    if market_data is not None:
        try:
            # Обёртка SDK 1.49 не принимает trade_source: сервер отдаёт все
            # сделки, небиржевые отбрасываются по полю сделки в _trades_to_rows.
            resp = await market_data.get_last_trades(instrument_id=figi)
            raw_trades = list(resp.trades or [])
        except Exception as exc:  # noqa: BLE001 - SDK raises typed gRPC errors
            logger.error("Ошибка получения сделок для %s: %s", instrument.id, exc)
            return BackfillResult.FAILED
    else:
        headers = {"Authorization": f"Bearer {config.token}", "accept": "application/json"}
        # SPEC-0002 G-2: по умолчанию сервер отдаёт и дилерские сделки (TRADE_SOURCE_ALL).
        payload = {"instrument_id": figi, "trade_source": "TRADE_SOURCE_EXCHANGE"}
        try:
            async with session.post(
                LAST_TRADES_URL,
                json=payload,
                headers=headers,
                timeout=aiohttp.ClientTimeout(total=120),
            ) as resp:
                if resp.status == 404:
                    logger.warning("Эндпоинт сделок недоступен для %s", instrument.id)
                    return BackfillResult.NO_DATA
                resp.raise_for_status()
                data = await resp.json()
                raw_trades = (data or {}).get("trades", []) or []
        except (aiohttp.ClientError, ValueError) as exc:
            logger.error("Ошибка получения сделок для %s: %s", instrument.id, exc)
            return BackfillResult.FAILED

    rows = _trades_to_rows(raw_trades, instrument)
    rows = [r for r in rows if _as_utc(r["timestamp"]).strftime("%Y-%m-%d") == day_str]
    if not rows:
        logger.info(
            "Нет сделок в окне для %s за %s (коллектор заполнит)",
            instrument.id,
            day_str,
        )
        return BackfillResult.NO_DATA
    await _persist_day(repo, day, rows)
    # GetLastTrades отдаёт лишь последние сделки: день перекачается из архива.
    await asyncio.to_thread(repo.mark_days_for_refresh, [day_str], REFRESH_PARTIAL_BACKFILL)
    return BackfillResult.WRITTEN


async def _persist_day(
    repo: MarketDataRepository,
    day: datetime,
    rows: list[dict[str, object]],
    replace: bool = False,
) -> None:
    """Направляет день в Parquet (холод) или SQLite (горячо).

    При ``replace=True`` строки дня в SQLite заменяются атомарно, а
    существующий Parquet-файл дня перезаписывается, чтобы хранилища не
    расходились.
    """
    day_str = day.strftime("%Y-%m-%d")
    df = pd.DataFrame(rows)
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
    df = df.dropna(subset=["timestamp"])
    if _target_for_day(day) == "parquet":
        if df.empty:
            return
        path = await asyncio.to_thread(repo.write_parquet, day_str, df)
        logger.info("Догружено %d строк в %s", len(df), path)
        return
    if replace:
        await asyncio.to_thread(repo.replace_hot_day, day_str, rows)
        if repo.parquet_day_path(day_str) is not None and not df.empty:
            await asyncio.to_thread(repo.write_parquet, day_str, df)
        logger.info("Перекачано %d строк в SQLite за %s", len(rows), day_str)
    else:
        await asyncio.to_thread(repo.insert_ticks, rows)
        logger.info("Догружено %d строк в SQLite за %s", len(rows), day_str)


def _history_rows(df: pd.DataFrame, instrument: Instrument) -> list[dict[str, object]]:
    """Преобразует строки архива истории в словари, готовые для репозитория.

    В архиве нет trade id, поэтому генерируется синтетический нечисловой id
    (почти уникальный на строку), который безопасно пропускает числовую
    проверку пропусков.
    """
    rows: list[dict[str, object]] = []
    for idx, rec in enumerate(df.itertuples(index=False)):
        ts = _as_utc(rec.timestamp)
        tid = str(getattr(rec, "trade_id", "") or "").strip()
        if not tid:
            tid = f"s{ts.strftime('%Y%m%d%H%M%S%f')}_{idx}"
        rows.append(
            {
                "ticker": instrument.storage_name,
                "timestamp": ts,
                "price": float(cast(Any, rec.price)),
                "volume": int(cast(Any, rec.volume)),
                "direction": parse_direction(rec.direction).value,
                "trade_id": tid,
            }
        )
    return rows


def archive_key_candidates(
    instrument: Instrument, day: datetime, futures: dict[str, FuturesMeta]
) -> list[str]:
    """Строит идентификаторы тикер/класс, которые принимает архив для инструмента.

    Непрерывные фьючерсы отслеживают контракт своего базового актива,
    активный в ``day`` (:func:`config.nearest_contract`) — то же правило, что
    у реального коллектора. Если такого контракта нет, ключей нет.
    """
    if instrument.type == InstrumentType.CONTINUOUS_FUTURES:
        meta = nearest_contract(instrument, futures.values(), day.date())
        return [] if meta is None else [f"{meta.ticker}_{meta.class_code}"]
    if instrument.type == InstrumentType.FUTURES_CONTRACT:
        found = futures.get(instrument.id)
        class_code = (
            found.class_code if found is not None else ARCHIVE_CLASS_CANDIDATES[instrument.type][0]
        )
        return [f"{instrument.id}_{class_code}"]
    return [f"{instrument.id}_{cc}" for cc in ARCHIVE_CLASS_CANDIDATES[instrument.type]]


def current_figi(
    instrument: Instrument,
    share_figis: dict[str, str | None],
    futures: dict[str, FuturesMeta],
    day: date,
) -> str | None:
    """FIGI инструмента для запроса сделок за ``day`` (сегодня); None, если не определён."""
    if instrument.type == InstrumentType.SHARE:
        return share_figis.get(instrument.id)
    if instrument.type == InstrumentType.FUTURES_CONTRACT:
        found = futures.get(instrument.id)
        return found.figi or None if found is not None else None
    meta = nearest_contract(instrument, futures.values(), day)
    return meta.figi or None if meta is not None else None


def _as_date(value: Any) -> date | None:
    """Разбирает ISO-строку даты (например, ``2026-09-18T17:00:00Z``) в date."""
    if not value:
        return None
    text = str(value).strip().replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(text).date()
    except ValueError:
        return None


def _trades_to_rows(raw_trades: list[Any], instrument: Instrument) -> list[dict[str, object]]:
    """Преобразует payloadы GetLastTrades (REST-словари или SDK Trade-объекты) в строки."""
    rows: list[dict[str, object]] = []
    dropped = 0
    for tr in raw_trades:
        source = (
            tr.get("trade_source", tr.get("tradeSource"))
            if isinstance(tr, dict)
            else (getattr(tr, "trade_source", None))
        )
        if not is_exchange_trade(source):  # SPEC-0002 G-1
            dropped += 1
            continue
        row = _trade_to_row(tr, instrument)
        if row is not None:
            rows.append(row)
    if dropped:
        logger.info("GetLastTrades %s: отброшено %d небиржевых сделок", instrument.id, dropped)
    return rows


def _exchange_only(df: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    """Оставляет только биржевые строки архива (SPEC-0002 G-2); возвращает и число отброшенных."""
    if "trade_source" not in df.columns:
        return df, 0
    keep = df["trade_source"].map(is_exchange_trade).astype(bool)
    return df.loc[keep].reset_index(drop=True), int((~keep).sum())


def _trade_to_row(raw: Any, instrument: Instrument) -> dict[str, object] | None:
    def g(key: str, default: Any = None) -> Any:
        if isinstance(raw, dict):
            return raw.get(key, default)
        return getattr(raw, key, default)

    ts = g("time") or g("timestamp")
    if ts is None:
        return None
    price_raw = g("price", 0)
    if isinstance(price_raw, dict):
        price = float(price_raw.get("units", 0)) + float(price_raw.get("nano", 0)) / 1e9
    elif hasattr(price_raw, "units"):  # SDK Quotation
        price = float(price_raw.units) + float(getattr(price_raw, "nano", 0)) / 1e9
    else:
        price = float(price_raw or 0)
    trade_id = str(g("trade_id") or g("tradeId") or "")
    return {
        "ticker": instrument.storage_name,
        "timestamp": _as_utc(ts),
        "price": price,
        "volume": int(g("quantity", g("volume", 0)) or 0),
        "direction": parse_direction(g("direction", "")).value,
        "trade_id": trade_id,
    }


def _as_utc(value: Any) -> datetime:
    """Приводит сырой API-таймстамп (str / datetime / float) к aware UTC datetime."""
    if value is None:
        return datetime.now(UTC)
    if isinstance(value, datetime):
        dt = value if value.tzinfo else value.replace(tzinfo=UTC)
        return dt.astimezone(UTC)
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(float(value), tz=UTC)
    text = str(value).strip().replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(text)
    except ValueError:
        return datetime.now(UTC)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)


async def run_backfill(
    config: AppConfig,
    repos: dict[str, MarketDataRepository],
    market_data: Any | None = None,
    instruments: Any | None = None,
    kind_share: Any = None,
    kind_futures: Any = None,
    futures_status: Any = None,
    *,
    gate: _RateGate | None = None,
    lock: asyncio.Lock | None = None,
    cleanup: bool = False,
) -> None:
    """Догружает отсутствующие и перекачивает отмеченные дни всех инструментов, с троттлингом.

    ``cleanup=True`` — отдельный проход чистки (SPEC-0002 G-4): только отметки
    ``dealer_cleanup``, без догрузки отсутствующих дней. ``gate`` — общий для
    процесса ограничитель темпа архива (:func:`archive_gate`). ``lock``, если
    задан, берётся на время обработки каждого дня, чтобы проход не пересекался
    с ночным обслуживанием, которое держит тот же замок целиком.

    Резолюция (FIGI акций, справочник фьючерсов) предпочитает инжектированный
    SDK ``InstrumentsService`` — REST-шлюз ``FindInstrument`` периодически
    отдаёт 404. ``futures_status`` должен включать истёкшие контракты
    (``INSTRUMENT_STATUS_ALL``), иначе для прошедших дней непрерывного
    фьючерса не найдётся контракт, активный в тот день. Данные дней берутся
    из официального архива истории через REST; для сегодняшнего дня
    предпочитается инжектированный gRPC ``market_data``. SDK здесь никогда не
    импортируется; сервисы приходят параметрами как duck-typed объекты.
    ``kind_futures`` сохранён для совместимости вызова.
    """
    del kind_futures
    semaphore = asyncio.Semaphore(MAX_BACKFILL_CONCURRENCY)
    rate_gate = gate if gate is not None else archive_gate()
    has_futures = any(i.type != InstrumentType.SHARE for i in config.instruments)
    today = _utc_today()

    async def process(
        session: aiohttp.ClientSession,
        instrument: Instrument,
        repo: MarketDataRepository,
        day: datetime,
        figi: str | None,
        replace: bool,
    ) -> None:
        archive_keys = archive_key_candidates(instrument, day, futures)
        async with semaphore, lock if lock is not None else contextlib.nullcontext():
            result = await backfill_day(
                session,
                config,
                instrument,
                repo,
                day,
                figi,
                archive_keys,
                market_data,
                rate_gate,
                replace=replace,
            )
            if replace:
                await asyncio.to_thread(_settle_refresh_mark, repo, day, result)

    async with aiohttp.ClientSession(
        connector=aiohttp.TCPConnector(ssl=_ssl_context()),
        timeout=aiohttp.ClientTimeout(total=120),
    ) as session:
        share_figis: dict[str, str | None] = {}
        for instrument in config.instruments:
            if instrument.type != InstrumentType.SHARE:
                continue
            if instruments is not None:
                share_figis[instrument.id] = await _resolve_share_figi_grpc(
                    instruments, instrument, kind_share
                )
            else:
                share_figis[instrument.id] = await _resolve_share_figi_rest(
                    session, config.token, instrument
                )
        futures: dict[str, FuturesMeta] = {}
        if has_futures:
            if instruments is not None:
                futures = await _fetch_futures_grpc(instruments, futures_status)
            else:
                futures = await _fetch_futures_rest(session, config.token)

        tasks = []
        for instrument in config.instruments:
            repo = repos[instrument.storage_name]
            figi = current_figi(instrument, share_figis, futures, today.date())
            missing = [] if cleanup else missing_days(config, repo)
            missing_set = {d.strftime("%Y-%m-%d") for d in missing}
            refresh = [
                d
                for d in refresh_days(config, repo, cleanup=cleanup)
                if d.strftime("%Y-%m-%d") not in missing_set
            ]
            for day in missing:
                tasks.append(process(session, instrument, repo, day, figi, replace=False))
            for day in refresh:
                tasks.append(process(session, instrument, repo, day, figi, replace=True))
        if tasks:
            await asyncio.gather(*tasks)

        # Отмеченные дни, которых нет ни в одном хранилище, обработаны как
        # отсутствующие; снимаем отметку, если данные появились или их нет у источника.
        for instrument in config.instruments:
            repo = repos[instrument.storage_name]
            marks = await asyncio.to_thread(repo.days_to_refresh)
            available = await asyncio.to_thread(repo.available_days)
            for day_str in marks:
                if day_str < today.strftime("%Y-%m-%d") and day_str not in available:
                    await asyncio.to_thread(repo.clear_refresh_mark, day_str)


def _settle_refresh_mark(repo: MarketDataRepository, day: datetime, result: BackfillResult) -> None:
    """Решает судьбу отметки после перекачки отмеченного дня.

    * ``WRITTEN`` — день заменён архивом, отметка снимается;
    * ``NO_EXCHANGE_TRADES`` — архив опубликован, биржевых сделок нет: день
      удаляется из хранилищ (там только небиржевые сделки), отметка снимается;
    * ``NO_DATA`` по отметке ``dealer_cleanup`` для дня, архив которого точно
      опубликован, — то же, что ``NO_EXCHANGE_TRADES`` (SPEC-0002 G-4);
    * ``NO_DATA`` при наличии сохранённых данных — архив за день ещё не
      опубликован, отметка остаётся до следующей попытки;
    * ``NO_DATA`` без данных — торгов не было, отметка снимается;
    * ``FAILED`` — отметка остаётся.
    """
    day_str = day.strftime("%Y-%m-%d")
    reason = repo.days_to_refresh().get(day_str)
    published = day <= _utc_today() - timedelta(days=ARCHIVE_PUBLISHED_AFTER_DAYS)
    confirmed_empty = result == BackfillResult.NO_EXCHANGE_TRADES or (
        result == BackfillResult.NO_DATA and reason == REFRESH_DEALER_CLEANUP and published
    )
    if result == BackfillResult.WRITTEN:
        repo.clear_refresh_mark(day_str)
        logger.info("День %s %s перекачан из архива", repo.instrument.storage_name, day_str)
    elif confirmed_empty:
        if day_str in repo.available_days():
            repo.delete_day(day_str)
            logger.warning(
                "День %s %s удалён: биржевых сделок по архиву нет (в хранилище были небиржевые)",
                repo.instrument.storage_name,
                day_str,
            )
        repo.clear_refresh_mark(day_str)
    elif result == BackfillResult.NO_DATA and day_str not in repo.available_days():
        repo.clear_refresh_mark(day_str)


def _pick_share(items: Iterable[Any], ticker: str) -> Any | None:
    """Акция с точным тикером; среди одноимённых — приоритет классов TQBR, затем SPBXM."""

    def field(item: Any, *names: str) -> Any:
        for name in names:
            value = item.get(name) if isinstance(item, dict) else getattr(item, name, None)
            if value:
                return value
        return None

    matches = [item for item in items if field(item, "ticker") == ticker]
    for preferred in SHARE_CLASS_PRIORITY:
        for item in matches:
            if field(item, "class_code", "classCode") == preferred:
                return item
    return matches[0] if matches else None


async def _resolve_share_figi_rest(
    session: aiohttp.ClientSession, token: str, instrument: Instrument
) -> str | None:
    """Разрешает тикер акции в FIGI через REST ``FindInstrument``; None при неудаче."""
    headers = {"Authorization": f"Bearer {token}", "accept": "application/json"}
    payload = {"query": instrument.id, "instrument_kind": 2}  # INSTRUMENT_TYPE_SHARE
    try:
        async with session.post(
            FIND_INSTRUMENT_URL,
            json=payload,
            headers=headers,
            timeout=aiohttp.ClientTimeout(total=30),
        ) as resp:
            resp.raise_for_status()
            data = await resp.json()
    except (aiohttp.ClientError, ValueError) as exc:
        logger.warning("Не удалось разрешить FIGI для %s: %s", instrument.id, exc)
        return None
    item = _pick_share(data.get("instruments", []) or [], instrument.id)
    if item is None:
        logger.warning("Акция %s не найдена через FindInstrument", instrument.id)
        return None
    return str(item.get("figi") or "") or None


async def _resolve_share_figi_grpc(
    instruments: Any, instrument: Instrument, kind_share: Any = None
) -> str | None:
    """Разрешает тикер акции в FIGI через SDK ``InstrumentsService``; None при неудаче."""
    try:
        resp = await instruments.find_instrument(query=instrument.id, instrument_kind=kind_share)
    except Exception as exc:  # noqa: BLE001 - SDK raises typed gRPC errors
        logger.warning("Не удалось разрешить FIGI для %s: %s", instrument.id, exc)
        return None
    item = _pick_share(list(resp.instruments or []), instrument.id)
    if item is None:
        logger.warning("Акция %s не найдена через FindInstrument", instrument.id)
        return None
    return str(getattr(item, "figi", "") or "") or None


async def _fetch_futures_grpc(
    instruments: Any, futures_status: Any = None
) -> dict[str, FuturesMeta]:
    """Список фьючерсов через SDK, индексируется по точному тикеру."""
    try:
        resp = await instruments.futures(instrument_status=futures_status)
    except Exception as exc:  # noqa: BLE001 - SDK raises typed gRPC errors
        logger.warning("Ошибка получения списка фьючерсов (gRPC): %s", exc)
        return {}
    return futures_from_sdk(resp.instruments or [])


def futures_from_sdk(items: Iterable[Any]) -> dict[str, FuturesMeta]:
    """Преобразует SDK-объекты ``Future`` в справочник ``{ticker: FuturesMeta}``."""
    metas: dict[str, FuturesMeta] = {}
    for item in items:
        ticker = getattr(item, "ticker", None)
        exp = _as_date(getattr(item, "expiration_date", None))
        if not ticker or exp is None:
            continue
        metas[str(ticker)] = FuturesMeta(
            ticker=str(ticker),
            class_code=str(getattr(item, "class_code", "SPBFUT") or "SPBFUT"),
            expiration=exp,
            figi=str(getattr(item, "figi", "") or ""),
        )
    return metas


async def _fetch_futures_rest(session: aiohttp.ClientSession, token: str) -> dict[str, FuturesMeta]:
    """REST-фолбэк для списка фьючерсов (окружения без SDK), включая истёкшие."""
    headers = {"Authorization": f"Bearer {token}", "accept": "application/json"}
    payload = {"instrument_status": 2}  # INSTRUMENT_STATUS_ALL
    try:
        async with session.post(
            FUTURES_URL, json=payload, headers=headers, timeout=aiohttp.ClientTimeout(total=30)
        ) as resp:
            resp.raise_for_status()
            data = await resp.json()
    except (aiohttp.ClientError, ValueError) as exc:
        logger.warning("Ошибка получения списка фьючерсов (REST): %s", exc)
        return {}
    metas: dict[str, FuturesMeta] = {}
    for item in data.get("instruments", []) or []:
        ticker = item.get("ticker")
        exp = _as_date(item.get("expirationDate"))
        if not ticker or exp is None:
            continue
        metas[str(ticker)] = FuturesMeta(
            ticker=str(ticker),
            class_code=str(item.get("classCode", "SPBFUT") or "SPBFUT"),
            expiration=exp,
            figi=str(item.get("figi", "") or ""),
        )
    return metas
