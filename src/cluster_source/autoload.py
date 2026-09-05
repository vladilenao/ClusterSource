"""Проверка полноты истории и автоматическая догрузка при старте.

Сканирует и холодное Parquet-хранилище, и горячее SQLite-хранилище на
заданное количество дней. Если день отсутствует — или был помечен как
скомпрометированный из-за пропуска trade_id — модуль перекачивает архив.

Основной источник истории: официальный сервис истории сделок, один файл на
инструмент и календарный день с анонимизированными сделками:
``GET https://invest-public-api.tbank.ru/history-trades/YYYY-MM-DD?instrumentId={TICKER_CLASS}``
(аутентификация Bearer). Ответ — gzip'нутый CSV (заголовок
``TRADE_TS,TICKER_CC,DIRECTION,PRICE,QUANTITY,TRADE_SOURCE,INSTRUMENT_UID``),
архивы обновляются каждую ночь и покрывают все прошедшие торговые дни.
404 = в этот день не было торгов. Шлюз также ограничивает загрузку
~30 файлами в минуту на IP.

Для текущего дня архив ещё не опубликован, поэтому догрузка откатывается на
аутентифицированный ``MarketDataService/GetLastTrades`` (только сделки текущей
сессии) — сначала через SDK-шный gRPC, когда он доступен, затем через REST.

Резолюция FIGI / контракта предпочитает SDK ``InstrumentsService`` (REST-шлюз
``FindInstrument`` периодически отдаёт 404); REST оставлен только как фолбэк
для окружений без SDK.

Легаси-архив публичных сделок (``https://tbank.ru/invest-api/data-api/...``)
прекратил работу (HTTP 404); возобновлять его НЕЛЬЗЯ.
"""

from __future__ import annotations

import asyncio
import csv
import gzip
import io
import logging
import ssl
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from typing import Any, cast

import aiohttp
import pandas as pd

from cluster_source.config import AppConfig, Instrument, InstrumentType
from cluster_source.database import HOT_RETENTION_DAYS, MarketDataRepository

logger = logging.getLogger(__name__)

HOT_RETENTION_DAYS = HOT_RETENTION_DAYS
REST_BASE = "https://invest-public-api.tinkoff.ru/rest/tinkoff.public.invest.api.contract.v1"
LAST_TRADES_URL = f"{REST_BASE}/MarketDataService/GetLastTrades"
HISTORY_TRADES_URL = "https://invest-public-api.tbank.ru/history-trades"
MAX_BACKFILL_CONCURRENCY = 4

#: Идентификаторы «тикер/класс», которые принимает архив истории для каждого
#: типа инструмента. Используются как фолбэк, когда код класса API не совпадает
#: (для акций конвенция архива отличается от ``classCode`` в find_instrument).
ARCHIVE_CLASS_CANDIDATES: dict[InstrumentType, list[str]] = {
    InstrumentType.SHARE: ["TQBR", "SPBXM"],
    InstrumentType.FUTURES_CONTRACT: ["SPBFUT"],
    InstrumentType.CONTINUOUS_FUTURES: ["SPBFUT"],
}


@dataclass(frozen=True)
class _FuturesMeta:
    """Минимальный дескриптор фьючерса для построения идентификаторов архива."""

    ticker: str
    class_code: str
    expiration: date


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
    today = datetime.now(UTC).replace(hour=0, minute=0, second=0, microsecond=0)
    available = repo.available_days()
    missing: list[datetime] = []
    for offset in range(config.history_check_days):
        day = today - timedelta(days=offset)
        day_str = day.strftime("%Y-%m-%d")
        if day_str not in available:
            missing.append(day)
    return missing


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

    parsed: list[dict[str, object]] = []
    for row in data:
        try:
            parsed.append(
                {
                    "timestamp": row[i_ts] if 0 <= i_ts < len(row) else "",
                    "price": float(row[i_price]) if 0 <= i_price < len(row) else 0.0,
                    "volume": int(row[i_vol]) if 0 <= i_vol < len(row) else 0,
                    "direction": (row[i_dir].upper() if 0 <= i_dir < len(row) else "BUY"),
                    "trade_id": row[i_tid] if 0 <= i_tid < len(row) else "",
                }
            )
        except (ValueError, IndexError):
            continue
    df = pd.DataFrame(parsed)
    if not df.empty and "timestamp" in df.columns:
        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
        df = df.dropna(subset=["timestamp"])
    return df


def _target_for_day(day: datetime) -> str:
    if day >= datetime.now(UTC).replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(
        days=HOT_RETENTION_DAYS
    ):
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
) -> bool:
    """Догружает один отсутствующий день.

    Прошедшие дни берутся из официального архива истории (``history-trades``)
    по кандидатным ключам тикер/класс; текущий день — из ``GetLastTrades``,
    потому что архив публикуется только на следующую ночь. Возвращает True
    при успехе, False при жёсткой ошибке.
    """
    day_str = day.strftime("%Y-%m-%d")
    today = datetime.now(UTC).date()
    if day.date() >= today:
        if figi is None:
            logger.error(
                "Невозможно догрузить %s %s: не удалось разрешить FIGI",
                instrument.id,
                day_str,
            )
            return False
        return await _backfill_current_session(
            session, config, instrument, repo, day, figi, market_data
        )
    if not archive_keys:
        logger.error(
            "Невозможно догрузить %s %s: ключ архива истории недоступен",
            instrument.id,
            day_str,
        )
        return False
    return await _backfill_from_archive(session, config, instrument, repo, day, archive_keys)


async def _backfill_from_archive(
    session: aiohttp.ClientSession,
    config: AppConfig,
    instrument: Instrument,
    repo: MarketDataRepository,
    day: datetime,
    archive_keys: list[str],
) -> bool:
    """Скачивает официальный дневной архив сделок (gzip-CSV) и сохраняет его."""
    day_str = day.strftime("%Y-%m-%d")
    headers = {"Authorization": f"Bearer {config.token}", "accept": "application/octet-stream"}

    content: bytes | None = None
    for key in archive_keys:
        url = f"{HISTORY_TRADES_URL}/{day_str}?instrumentId={key}"
        logger.info("Загрузка архива истории %s для %s (%s)", key, instrument.id, day_str)
        try:
            async with session.get(
                url, headers=headers, timeout=aiohttp.ClientTimeout(total=120)
            ) as resp:
                if resp.status == 404:
                    continue
                if resp.status == 429:  # ~30 загрузок в минуту на IP
                    await asyncio.sleep(10)
                    resp2 = await session.get(
                        url, headers=headers, timeout=aiohttp.ClientTimeout(total=120)
                    )
                    resp = resp2
                    if resp.status == 429:
                        logger.warning(
                            "Архив истории ограничил лимит запросов для %s за %s",
                            instrument.id,
                            day_str,
                        )
                        return True
                resp.raise_for_status()
                content = await resp.read()
                break
        except aiohttp.ClientError as exc:
            logger.error("Ошибка загрузки архива истории для %s: %s", instrument.id, exc)
            return False

    if content is None:
        logger.info(
            "Архива истории нет для %s за %s (в этот день не было торгов)",
            instrument.id,
            day_str,
        )
        return True

    if content[:2] == b"\x1f\x8b":
        try:
            content = gzip.decompress(content)
        except OSError as exc:
            logger.error("Повреждён архив истории для %s: %s", instrument.id, exc)
            return False

    df = _parse_history_csv(content)
    if df.empty:
        logger.info("Нет сделок в архиве для %s за %s", instrument.id, day_str)
        return True

    rows = _history_rows(df, instrument)
    rows = [r for r in rows if _as_utc(r["timestamp"]).strftime("%Y-%m-%d") == day_str]
    if not rows:
        logger.info("Нет сделок в окне для %s за %s", instrument.id, day_str)
        return True
    return await _persist_day(repo, day, rows)


async def _backfill_current_session(
    session: aiohttp.ClientSession,
    config: AppConfig,
    instrument: Instrument,
    repo: MarketDataRepository,
    day: datetime,
    figi: str,
    market_data: Any | None,
) -> bool:
    """Получает сделки текущей сессии (GetLastTrades) и сохраняет день."""
    day_str = day.strftime("%Y-%m-%d")
    raw_trades: list[Any] = []
    if market_data is not None:
        try:
            resp = await market_data.get_last_trades(instrument_id=figi)
            raw_trades = list(resp.trades or [])
        except Exception as exc:  # noqa: BLE001 - SDK raises typed gRPC errors
            logger.error("Ошибка получения сделок для %s: %s", instrument.id, exc)
            return False
    else:
        headers = {"Authorization": f"Bearer {config.token}", "accept": "application/json"}
        payload = {"instrument_id": figi, "count": 100}
        try:
            async with session.post(
                LAST_TRADES_URL,
                json=payload,
                headers=headers,
                timeout=aiohttp.ClientTimeout(total=120),
            ) as resp:
                if resp.status == 404:
                    logger.warning("Эндпоинт сделок недоступен для %s", instrument.id)
                    return True
                resp.raise_for_status()
                data = await resp.json()
                raw_trades = (data or {}).get("trades", []) or []
        except (aiohttp.ClientError, ValueError) as exc:
            logger.error("Ошибка получения сделок для %s: %s", instrument.id, exc)
            return False

    rows = _trades_to_rows(raw_trades, instrument)
    rows = [r for r in rows if _as_utc(r["timestamp"]).strftime("%Y-%m-%d") == day_str]
    if not rows:
        logger.info(
            "Нет сделок в окне для %s за %s (коллектор заполнит)",
            instrument.id,
            day_str,
        )
        return True
    return await _persist_day(repo, day, rows)


async def _persist_day(
    repo: MarketDataRepository, day: datetime, rows: list[dict[str, object]]
) -> bool:
    """Направляет полностью заполненный день в Parquet (холод) или SQLite (горячо)."""
    day_str = day.strftime("%Y-%m-%d")
    if _target_for_day(day) == "parquet":
        df = pd.DataFrame(rows)
        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
        df = df.dropna(subset=["timestamp"])
        if df.empty:
            return True
        path = await asyncio.to_thread(repo.write_parquet, day_str, df)
        logger.info("Догружено %d строк в %s", len(df), path)
    else:
        await asyncio.to_thread(repo.insert_ticks, rows)
        logger.info("Догружено %d строк в SQLite за %s", len(rows), day_str)
    return True


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
                "direction": str(rec.direction).upper(),
                "trade_id": tid,
            }
        )
    return rows


def _archive_key_candidates(
    instrument: Instrument, day: datetime, futures: dict[str, _FuturesMeta]
) -> list[str]:
    """Строит идентификаторы тикер/класс, которые принимает архив для инструмента.

    Непрерывные фьючерсы отслеживают контракт, активный в ``day`` (ближайший
    по истечению, на/после дня), зеркаля выбор «текущего ликвидного контракта»
    реальным коллектором.
    """
    if instrument.type == InstrumentType.CONTINUOUS_FUTURES:
        meta = _nearest_future(futures, day.date())
        if meta is not None:
            return [f"{meta.ticker}_{meta.class_code}"]
    if instrument.type == InstrumentType.FUTURES_CONTRACT:
        meta = (futures or {}).get(instrument.id)
        class_code = (
            meta.class_code if meta is not None else ARCHIVE_CLASS_CANDIDATES[instrument.type][0]
        )
        return [f"{instrument.id}_{class_code}"]
    return [f"{instrument.id}_{cc}" for cc in ARCHIVE_CLASS_CANDIDATES[instrument.type]]


def _nearest_future(futures: dict[str, _FuturesMeta], day: date) -> _FuturesMeta | None:
    """Ближайший фьючерс, истекающий на/после ``day`` (иначе — самый дальний)."""
    metas = list((futures or {}).values())
    if not metas:
        return None
    eligible = [m for m in metas if m.expiration >= day]
    pool = eligible or metas
    return min(pool, key=lambda m: m.expiration)


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
    for tr in raw_trades:
        row = _trade_to_row(tr, instrument)
        if row is not None:
            rows.append(row)
    return rows


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
    direction = str(g("direction", "")).upper()
    direction = "BUY" if direction in ("1", "BUY", "TRADE_DIRECTION_BUY") else "SELL"
    trade_id = str(g("trade_id") or g("tradeId") or "")
    return {
        "ticker": instrument.storage_name,
        "timestamp": _as_utc(ts),
        "price": price,
        "volume": int(g("quantity", g("volume", 0)) or 0),
        "direction": direction,
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
) -> None:
    """Проверяет все инструменты и догружает каждый отсутствующий день, с троттлингом.

    Резолюция (FIGI, маппинг фьючерсов) предпочитает инжектированный SDK
    ``InstrumentsService`` — REST-шлюз ``FindInstrument`` периодически отдаёт
    404. Данные дней берутся из официального архива истории через REST (хост
    архива стабилен); для сегодняшнего дня дополнительно предпочитается
    инжектированный gRPC ``market_data`` для сделок текущей сессии. SDK здесь
    никогда не импортируется; сервисы приходят через параметры конструктора
    как duck-typed протокол-объекты.
    """
    semaphore = asyncio.Semaphore(MAX_BACKFILL_CONCURRENCY)
    has_futures = any(
        instrument.type in (InstrumentType.FUTURES_CONTRACT, InstrumentType.CONTINUOUS_FUTURES)
        for instrument in config.instruments
    )

    async def throttled(
        session: aiohttp.ClientSession,
        instrument: Instrument,
        repo: MarketDataRepository,
        day: datetime,
        figi: str | None,
        archive_keys: list[str],
    ) -> None:
        async with semaphore:
            await backfill_day(
                session, config, instrument, repo, day, figi, archive_keys, market_data
            )

    async with aiohttp.ClientSession(
        connector=aiohttp.TCPConnector(ssl=_ssl_context()),
        timeout=aiohttp.ClientTimeout(total=120),
    ) as resolve_session:
        figis: dict[str, str | None] = {}
        futures: dict[str, _FuturesMeta] = {}
        for instrument in config.instruments:
            if instruments is not None:
                figis[instrument.id] = await _resolve_figi_grpc(
                    instruments, instrument, kind_share, kind_futures
                )
            else:
                figis[instrument.id] = await _resolve_figi(
                    resolve_session, config.token, instrument
                )
        if has_futures:
            if instruments is not None:
                futures = await _fetch_futures_grpc(instruments, futures_status)
            else:
                futures = await _fetch_futures_rest(resolve_session, config.token)

        async with aiohttp.ClientSession(
            connector=aiohttp.TCPConnector(ssl=_ssl_context()),
            timeout=aiohttp.ClientTimeout(total=120),
        ) as data_session:
            tasks = []
            for instrument in config.instruments:
                repo = repos[instrument.storage_name]
                for day in missing_days(config, repo):
                    archive_keys = _archive_key_candidates(instrument, day, futures)
                    tasks.append(
                        asyncio.create_task(
                            throttled(
                                data_session,
                                instrument,
                                repo,
                                day,
                                figis[instrument.id],
                                archive_keys,
                            )
                        )
                    )
            if tasks:
                await asyncio.gather(*tasks)


REST_KIND: dict[InstrumentType, int] = {
    InstrumentType.SHARE: 2,
    InstrumentType.FUTURES_CONTRACT: 5,
    InstrumentType.CONTINUOUS_FUTURES: 5,
}


async def _resolve_figi(
    session: aiohttp.ClientSession, token: str, instrument: Instrument
) -> str | None:
    """Разрешает тикер в FIGI через поиск инструмента T-Investments.

    Откатывается на сам тикер, если резолюция недоступна.
    """
    url = (
        "https://invest-public-api.tinkoff.ru/rest/tinkoff.public.invest."
        "api.contract.v1.InstrumentsService/FindInstrument"
    )
    headers = {"Authorization": f"Bearer {token}", "accept": "application/json"}
    payload = {
        "query": instrument.id,
        "instrument_kind": REST_KIND.get(instrument.type, 2),
    }
    try:
        async with session.post(
            url, json=payload, headers=headers, timeout=aiohttp.ClientTimeout(total=30)
        ) as resp:
            resp.raise_for_status()
            data = await resp.json()
            instruments = data.get("instruments", []) or []
            # Предпочитаем точное совпадение тикера (FindInstrument может вернуть
            # связанные инструменты первыми, например SBERP для «SBER»).
            for item in instruments:
                if item.get("ticker") == instrument.id:
                    return item.get("figi") or instrument.id
            if instruments:
                return instruments[0].get("figi") or instrument.id
    except (aiohttp.ClientError, ValueError) as exc:
        logger.warning("Не удалось разрешить FIGI для %s: %s", instrument.id, exc)
    return instrument.id


async def _resolve_figi_grpc(
    instruments: Any,
    instrument: Instrument,
    kind_share: Any = None,
    kind_futures: Any = None,
) -> str | None:
    """Разрешает тикер в FIGI через SDK ``InstrumentsService``.

    Предпочитает точное совпадение тикера (find_instrument может вернуть
    связанные инструменты первыми, например SBERP для «SBER»). Возвращает
    None при неудаче.
    """
    kind = kind_share if instrument.type == InstrumentType.SHARE else kind_futures
    try:
        resp = await instruments.find_instrument(query=instrument.id, instrument_kind=kind)
    except Exception as exc:  # noqa: BLE001 - SDK raises typed gRPC errors
        logger.warning("Не удалось разрешить FIGI для %s: %s", instrument.id, exc)
        return None
    items = list(resp.instruments or [])
    for item in items:
        if getattr(item, "ticker", None) == instrument.id:
            figi = getattr(item, "figi", None) or instrument.id
            return str(figi)
    first = items[0] if items else None
    if first is None:
        return None
    return str(getattr(first, "figi", None) or instrument.id)


async def _fetch_futures_grpc(
    instruments: Any, futures_status: Any = None
) -> dict[str, _FuturesMeta]:
    """Список всех фьючерсов через SDK, индексируется по точному тикеру."""
    try:
        resp = await instruments.futures(instrument_status=futures_status)
    except Exception as exc:  # noqa: BLE001 - SDK raises typed gRPC errors
        logger.warning("Ошибка получения списка фьючерсов (gRPC): %s", exc)
        return {}
    metas: dict[str, _FuturesMeta] = {}
    for item in resp.instruments or []:
        ticker = getattr(item, "ticker", None)
        expiration = getattr(item, "expiration_date", None)
        if not ticker or expiration is None:
            continue
        exp = _as_date(expiration)
        if exp is None:
            continue
        metas[str(ticker)] = _FuturesMeta(
            str(ticker), str(getattr(item, "class_code", "SPBFUT") or "SPBFUT"), exp
        )
    return metas


async def _fetch_futures_rest(
    session: aiohttp.ClientSession, token: str
) -> dict[str, _FuturesMeta]:
    """REST-фолбэк для списка фьючерсов (окружения без SDK)."""
    url = f"{REST_BASE}/InstrumentsService/Futures"
    headers = {"Authorization": f"Bearer {token}", "accept": "application/json"}
    payload = {"instrument_status": 1}
    try:
        async with session.post(
            url, json=payload, headers=headers, timeout=aiohttp.ClientTimeout(total=30)
        ) as resp:
            resp.raise_for_status()
            data = await resp.json()
    except (aiohttp.ClientError, ValueError) as exc:
        logger.warning("Ошибка получения списка фьючерсов (REST): %s", exc)
        return {}
    metas: dict[str, _FuturesMeta] = {}
    for item in data.get("instruments", []) or []:
        ticker = item.get("ticker")
        exp = _as_date(item.get("expirationDate"))
        if not ticker or exp is None:
            continue
        metas[str(ticker)] = _FuturesMeta(
            str(ticker), str(item.get("classCode", "SPBFUT") or "SPBFUT"), exp
        )
    return metas
