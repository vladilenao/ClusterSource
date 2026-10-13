"""Сбор ленты Мосбиржи из ISS (SPEC-0003, этап 2).

Бесплатный ISS отдаёт сделки только текущего торгового дня с задержкой
15 минут, поэтому ленту нужно забирать каждый день: номера сделок
(``TRADENO``) и открытый интерес по сделкам FORTS (``OPENPOSITION``) есть
только здесь и за пропущенный день теряются безвозвратно.

:class:`IssCollector` раз в ``iss.poll_interval_sec`` догоняет общие ленты
рынка акций и FORTS страницами по курсору (``tradeno`` / ``recno`` с
``next_trade=1``) и сохраняет нормализованные
сделки через :class:`~cluster_source.database.IssTapeRepository`. Смена дня в
ISS определяется по смене первой сделки ленты: прежний день выгружается в
Parquet, а если сборщик не успел его догнать, день помечается неполным
(SPEC-0003 G-2). Раз в час:

* выгружаются в Parquet «зависшие» в SQLite ленты (после падения процесса);
* импортируются сырые файлы временного сборщика этапа 1
  (``_iss_raw/{YYYY-MM-DD}/{market}.csv.gz``) за завершённые дни;
* полнота каждого торгового дня сверяется с дневными итогами ISS
  (``NUMTRADES`` по бумагам, SPEC-0003 G-5).

Параметр ``start`` в ISS устарел: на глубоких смещениях (от ~1,3 млн строк)
ISS отвечает пустыми страницами, и 2026-10-12 обе выгрузки застряли посреди
дня. Курсор по номеру сделки (акции) или ``RECNO`` (FORTS) этого не имеет.
Конец ленты — пустой ответ, подтверждённый повторным запросом; ложный конец
ничего не теряет: следующий цикл продолжает с того же курсора.
"""

from __future__ import annotations

import asyncio
import csv
import gzip
import logging
import zlib
from collections.abc import Awaitable, Callable, Iterator, Sequence
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path
from typing import Any, Protocol
from zoneinfo import ZoneInfo

import aiohttp

from cluster_source.autoload import _ssl_context
from cluster_source.config import AppConfig, Direction
from cluster_source.database import IssTapeRepository

logger = logging.getLogger(__name__)

ISS_BASE = "https://iss.moex.com/iss"
#: Общие ленты сделок рынков.
TRADES_URLS: dict[str, str] = {
    "shares": f"{ISS_BASE}/engines/stock/markets/shares/trades.json",
    "forts": f"{ISS_BASE}/engines/futures/markets/forts/trades.json",
}
#: Дневные итоги по бумагам рынка (``NUMTRADES``) за торговый день.
HISTORY_URLS: dict[str, str] = {
    "shares": f"{ISS_BASE}/history/engines/stock/markets/shares/boards/TQBR/securities.json",
    "forts": f"{ISS_BASE}/history/engines/futures/markets/forts/securities.json",
}
#: Режим торгов, по которому сверяется полнота (G-5).
CHECK_BOARDS: dict[str, str] = {"shares": "TQBR", "forts": "RFUD"}

PAGE = 5000
PAGE_PAUSE_SEC = 0.3
#: Пустой ответ — конец ленты; на случай сбоя ISS он перепроверяется столько раз.
EMPTY_CONFIRMATIONS = 2
EMPTY_RETRY_SEC = 2.0
#: Курсор ленты: номер сделки на фондовом рынке (монотонен), RECNO на FORTS
#: (TRADENO там не монотонен). Параметр ``start`` в ISS устарел: на глубоких
#: смещениях (от ~1,3 млн строк) ISS отвечает пустыми страницами.
CURSOR_COLUMNS: dict[str, str] = {"shares": "TRADENO", "forts": "RECNO"}
CURSOR_PARAMS: dict[str, str] = {"shares": "tradeno", "forts": "recno"}
LOG_EVERY_ROWS = 100_000
HOUSEKEEPING_INTERVAL_SEC = 3600.0
HTTP_ATTEMPTS = 6
MAX_HTTP_BACKOFF_SEC = 60.0
RAW_IMPORT_CHUNK = 50_000

#: Торговый день D сверяется не раньше 06:00 МСК следующих суток (итоги опубликованы).
VERIFY_NOT_BEFORE = timedelta(days=1, hours=6)
#: Сверяются торговые дни не старше стольких суток.
VERIFY_LOOKBACK_DAYS = 10
#: Если итогов за день нет и через столько суток — день не сверить.
VERIFY_GIVE_UP_DAYS = 3
#: Ленты сессий выходного и праздничных дней относятся к торговому дню позже
#: своей даты: при подсчёте за день D смотрим ленты с этой глубиной.
FEED_LOOKBACK_DAYS = 10

MSK = ZoneInfo("Europe/Moscow")

KIND_FEED = "feed"
KIND_TRADE_DATE = "trade_date"
STATUS_COMPLETE = "complete"
STATUS_INCOMPLETE = "incomplete"
STATUS_UNVERIFIABLE = "unverifiable"

_DIRECTIONS = {"B": Direction.BUY, "S": Direction.SELL}


class IssUnavailable(RuntimeError):
    """ISS не ответил после всех повторов."""


class IssClient(Protocol):
    """Источник JSON-ответов ISS; в тестах подменяется заглушкой."""

    async def get_json(self, url: str, params: dict[str, str]) -> dict[str, Any]: ...


class HttpIssClient:
    """HTTP-клиент ISS с повторами и экспоненциальной паузой."""

    def __init__(
        self,
        session: aiohttp.ClientSession,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        self._session = session
        self._sleep = sleep

    async def get_json(self, url: str, params: dict[str, str]) -> dict[str, Any]:
        for attempt in range(HTTP_ATTEMPTS):
            try:
                async with self._session.get(url, params=params) as resp:
                    resp.raise_for_status()
                    data: dict[str, Any] = await resp.json(content_type=None)
                    return data
            except (aiohttp.ClientError, TimeoutError, ValueError) as exc:
                wait = min(MAX_HTTP_BACKOFF_SEC, 2.0**attempt)
                logger.warning("ISS: ошибка запроса %s (%s); повтор через %.0f с", url, exc, wait)
                await self._sleep(wait)
        raise IssUnavailable(url)


# --------------------------------------------------------------------------- #
# Нормализация строк ленты
# --------------------------------------------------------------------------- #
def _opt_int(value: object) -> int | None:
    if value is None or value == "":
        return None
    return int(str(value)) if not isinstance(value, int | float) else int(value)


def _opt_str(value: object) -> str | None:
    return None if value is None or value == "" else str(value)


#: Сделка записана «вовремя», если SYSTIME не раньше её времени (с допуском) и не позже часа.
_RECORDED_LAG_MAX = timedelta(hours=1)
_CLOCK_SKEW = timedelta(minutes=5)


def trade_time_utc(systime: str, tradetime: str, tradedate: str | None = None) -> datetime:
    """Время сделки в UTC по полям ISS (все — время МСК).

    ``TRADETIME`` — время сделки без даты. Дата берётся из ``SYSTIME`` (момент
    записи), если сделка записана вовремя: так верно определяется дата при
    переходе через полночь и для сессий выходного дня на рынке акций. Если
    запись заметно позже сделки — например, сделки сессии выходного дня FORTS
    приходят пачкой в понедельник 06:00 — дата берётся из ``TRADEDATE``
    (календарная дата сделки; торговый день — ``TRADE_SESSION_DATE``).
    """
    recorded = datetime.fromisoformat(systime).replace(tzinfo=MSK)
    clock = time.fromisoformat(tradetime)
    for day in (recorded.date(), recorded.date() - timedelta(days=1)):
        moment = datetime.combine(day, clock, tzinfo=MSK)
        if recorded - _RECORDED_LAG_MAX <= moment <= recorded + _CLOCK_SKEW:
            return moment.astimezone(UTC)
    if tradedate:
        return datetime.combine(date.fromisoformat(tradedate), clock, tzinfo=MSK).astimezone(UTC)
    return datetime.combine(recorded.date(), clock, tzinfo=MSK).astimezone(UTC)


def normalize_trades(
    market: str, feed_date: str, columns: Sequence[str], rows: Sequence[Sequence[object]]
) -> list[dict[str, object]]:
    """Преобразует строки ISS (JSON или CSV этапа 1) в строки ``iss_trades``."""
    idx = {name: i for i, name in enumerate(columns)}

    def get(row: Sequence[object], name: str) -> object:
        i = idx.get(name)
        return row[i] if i is not None and i < len(row) else None

    board_col = "BOARDID" if "BOARDID" in idx else "BOARDNAME"
    out: list[dict[str, object]] = []
    for row in rows:
        tradeno = _opt_int(get(row, "TRADENO"))
        if tradeno is None:
            continue
        out.append(
            {
                "market": market,
                "feed_date": feed_date,
                "secid": str(get(row, "SECID")),
                "board": str(get(row, board_col)),
                "tradeno": tradeno,
                "timestamp": trade_time_utc(
                    str(get(row, "SYSTIME")),
                    str(get(row, "TRADETIME")),
                    _opt_str(get(row, "TRADEDATE")),
                ).isoformat(timespec="microseconds"),
                # Торговый день биржи: сделки сессии выходного дня относятся к следующему.
                "trade_date": str(get(row, "TRADE_SESSION_DATE") or get(row, "TRADEDATE")),
                "price": float(str(get(row, "PRICE"))),
                "quantity": int(float(str(get(row, "QUANTITY")))),
                "direction": _DIRECTIONS.get(
                    str(get(row, "BUYSELL") or "").upper(), Direction.UNKNOWN
                ).value,
                "period": _opt_str(get(row, "PERIOD")),
                "session": _opt_str(get(row, "TRADINGSESSION")),
                "open_position": _opt_int(get(row, "OPENPOSITION")),
                "off_market": _opt_int(get(row, "OFFMARKETDEAL")),
            }
        )
    return out


def _head(columns: Sequence[str], row: Sequence[object]) -> tuple[str, str]:
    """Ключ ленты (первая сделка) и её ``feed_date`` — дата ``SYSTIME`` первой сделки."""
    systime = str(row[columns.index("SYSTIME")])
    return f"{row[columns.index('TRADENO')]}|{systime}", systime[:10]


# --------------------------------------------------------------------------- #
# Сборщик
# --------------------------------------------------------------------------- #
class IssCollector:
    """Держит актуальной ленту ISS обоих рынков и проверяет полноту дней."""

    def __init__(
        self,
        config: AppConfig,
        repo: IssTapeRepository,
        client: IssClient | None = None,
        *,
        now: Callable[[], datetime] = lambda: datetime.now(UTC),
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        self._config = config
        self._repo = repo
        self._client = client
        self._now = now
        self._sleep = sleep
        self._raw_dir = config.storage_root / "_iss_raw"

    async def run(self) -> None:
        """Бесконечный цикл сбора; ошибки рынка логируются и не останавливают цикл."""
        if self._client is not None:
            await self._loop()
            return
        async with aiohttp.ClientSession(
            connector=aiohttp.TCPConnector(ssl=_ssl_context()),  # SPEC-0003 G-3
            timeout=aiohttp.ClientTimeout(total=120),
        ) as session:
            self._client = HttpIssClient(session, self._sleep)
            try:
                await self._loop()
            finally:
                self._client = None

    async def _loop(self) -> None:
        next_housekeeping = 0.0
        loop = asyncio.get_running_loop()
        while True:
            for market in TRADES_URLS:
                try:
                    await self.poll_market(market)
                except Exception as exc:  # noqa: BLE001 - не роняем сборщик из-за одного рынка
                    logger.error("ISS %s: ошибка цикла сбора: %s", market, exc)
            if loop.time() >= next_housekeeping:
                try:
                    await self.housekeeping()
                except Exception as exc:  # noqa: BLE001
                    logger.error("ISS: ошибка обслуживания ленты: %s", exc)
                next_housekeeping = loop.time() + HOUSEKEEPING_INTERVAL_SEC
            await self._sleep(self._config.iss.poll_interval_sec)

    async def housekeeping(self) -> None:
        """Выгрузка зависших лент, импорт сырых файлов этапа 1, проверка полноты."""
        await self.finalize_stale_feeds()
        await self.import_raw()
        await self.verify_pending()

    # ------------------------------------------------------------------ #
    # Догонка ленты
    # ------------------------------------------------------------------ #
    async def _fetch(
        self, market: str, cursor: str | None, limit: int
    ) -> tuple[list[str], list[list[object]]]:
        """Страница ленты после сделки ``cursor`` (или с начала ленты)."""
        assert self._client is not None
        params = {"iss.meta": "off", "iss.only": "trades", "limit": str(limit)}
        if cursor is not None:
            params[CURSOR_PARAMS[market]] = cursor
            params["next_trade"] = "1"
        data = await self._client.get_json(TRADES_URLS[market], params)
        trades = data.get("trades") or {}
        return list(trades.get("columns") or []), list(trades.get("data") or [])

    async def poll_market(self, market: str) -> None:
        """Догоняет ленту рынка с сохранённого курсора; обрабатывает смену дня."""
        columns, first = await self._fetch(market, None, 1)
        if not first:
            return
        head, feed_date = _head(columns, first[0])
        state = await asyncio.to_thread(self._repo.feed_state, market)
        if state is None or state["head"] != head:
            if state is not None:
                await self._finish_feed(market, state)
                logger.info("ISS %s: новая лента за %s", market, feed_date)
            await asyncio.to_thread(self._repo.start_feed, market, head, feed_date)
            state = {"offset": 0, "last": None, "feed_date": feed_date}
        last = state["last"]
        await self._catch_up(
            market, feed_date, int(str(state["offset"])), None if last is None else str(last)
        )

    async def _catch_up(self, market: str, feed_date: str, offset: int, cursor: str | None) -> bool:
        """Читает ленту от курсора до конца; возвращает True, если конец достигнут.

        Каждая страница и курсор сохраняются в одной транзакции. Конец ленты —
        пустой ответ, подтверждённый ``EMPTY_CONFIRMATIONS`` раз.
        """
        added = 0
        empty = 0
        reached_end = False
        while True:
            columns, rows = await self._fetch(market, cursor, PAGE)
            if not rows:
                if empty >= EMPTY_CONFIRMATIONS:
                    reached_end = True
                    break
                empty += 1
                await self._sleep(EMPTY_RETRY_SEC)
                continue
            empty = 0
            offset += len(rows)
            cursor = str(rows[-1][columns.index(CURSOR_COLUMNS[market])])
            normalized = normalize_trades(market, feed_date, columns, rows)
            await asyncio.to_thread(self._repo.append_page, market, normalized, offset, cursor)
            added += len(rows)
            if added % LOG_EVERY_ROWS < len(rows):
                logger.info(
                    "ISS %s %s: догоняю ленту, +%d, всего %d", market, feed_date, added, offset
                )
            await self._sleep(PAGE_PAUSE_SEC)
        await asyncio.to_thread(self._repo.set_caught_up, market, reached_end)
        if added:
            logger.info("ISS %s %s: +%d сделок, всего %d", market, feed_date, added, offset)
        return reached_end

    async def _finish_feed(self, market: str, state: dict[str, object]) -> None:
        """Закрывает прежнюю ленту при смене дня в ISS.

        Догнать прежний день уже нельзя: ``previous_session=1`` у FORTS отдаёт
        лишь начало прошлой сессии (проверено 2026-10-13: до ~07:10 МСК), а не
        весь день. Поэтому недогнанная лента — SPEC-0003 G-2.
        """
        feed_date = str(state["feed_date"])
        detail = f"rows={state['offset']}"
        if state["caught_up"]:
            status = STATUS_COMPLETE
        else:
            status = STATUS_INCOMPLETE
            logger.error(
                "ISS_FEED_INCOMPLETE %s %s: ISS перешёл на новый день до догонки ленты (%s);"
                " номера сделок и ОИ за остаток дня потеряны",
                market,
                feed_date,
                detail,
            )
        await asyncio.to_thread(
            self._repo.set_day_status, market, feed_date, KIND_FEED, status, detail
        )
        await asyncio.to_thread(self._repo.finalize_feed_day, market, feed_date)

    async def finalize_stale_feeds(self) -> None:
        """Выгружает в Parquet ленты, оставшиеся в SQLite после падения процесса."""
        for market in TRADES_URLS:
            state = await asyncio.to_thread(self._repo.feed_state, market)
            current = None if state is None else state["feed_date"]
            for feed_date in sorted(await asyncio.to_thread(self._repo.staged_feed_dates, market)):
                if feed_date != current:
                    await asyncio.to_thread(self._repo.finalize_feed_day, market, feed_date)

    # ------------------------------------------------------------------ #
    # Импорт сырых файлов этапа 1
    # ------------------------------------------------------------------ #
    async def import_raw(self) -> None:
        """Импортирует сырые файлы ``_iss_raw/{date}/{market}*.csv.gz`` завершённых дней.

        День завершён, только когда сам сборщик уже видит более новую ленту
        рынка: до смены дня в ISS сырой файл ещё дописывается. Файл
        импортируется повторно, если его размер изменился (дубли отсекает
        ключ уникальности).
        """
        if not self._raw_dir.exists():
            return
        for day_dir in sorted(p for p in self._raw_dir.iterdir() if p.is_dir()):
            day = day_dir.name
            try:
                date.fromisoformat(day)
            except ValueError:
                continue
            for market in TRADES_URLS:
                state = await asyncio.to_thread(self._repo.feed_state, market)
                if state is None or str(state["feed_date"]) <= day:
                    continue  # лента дня может ещё идти: импорт после смены дня
                for path in sorted(day_dir.glob(f"{market}*.csv.gz")):
                    await self._import_raw_path(market, day, path)

    async def _import_raw_path(self, market: str, day: str, path: Path) -> None:
        key = f"raw_imported:{market}:{day}:{path.name}"
        size = str(path.stat().st_size)
        if await asyncio.to_thread(self._repo.get_meta, key) == size:
            return
        rows, truncated = await asyncio.to_thread(self._import_raw_file, market, day, path)
        await asyncio.to_thread(self._repo.finalize_feed_day, market, day)
        await asyncio.to_thread(self._repo.set_meta, key, size)
        if truncated:
            await asyncio.to_thread(
                self._repo.set_day_status,
                market,
                day,
                KIND_FEED,
                STATUS_INCOMPLETE,
                "raw_truncated",
            )
        logger.info(
            "ISS %s %s: импортирован сырой файл %s (%d строк%s)",
            market,
            day,
            path.name,
            rows,
            ", файл обрезан" if truncated else "",
        )

    def _import_raw_file(self, market: str, day: str, path: Path) -> tuple[int, bool]:
        total = 0
        truncated = False
        try:
            for columns, chunk in _read_raw_chunks(path):
                self._repo.stage_rows(normalize_trades(market, day, columns, chunk))
                total += len(chunk)
        except (EOFError, gzip.BadGzipFile, zlib.error, csv.Error) as exc:
            truncated = True
            logger.warning(
                "ISS %s %s: сырой файл %s прочитан не полностью: %s", market, day, path, exc
            )
        return total, truncated

    # ------------------------------------------------------------------ #
    # Проверка полноты (G-5)
    # ------------------------------------------------------------------ #
    async def verify_pending(self) -> None:
        """Сверяет число сделок по бумагам с ``NUMTRADES`` дневных итогов ISS."""
        now_msk = self._now().astimezone(MSK)
        today = now_msk.date()
        oldest = today - timedelta(days=VERIFY_LOOKBACK_DAYS)
        since = (oldest - timedelta(days=FEED_LOOKBACK_DAYS)).isoformat()
        for market in TRADES_URLS:
            for trade_date in sorted(
                await asyncio.to_thread(self._repo.trade_dates, market, since)
            ):
                day = date.fromisoformat(trade_date)
                ready_at = datetime.combine(day, time(0), tzinfo=MSK) + VERIFY_NOT_BEFORE
                if day < oldest or now_msk < ready_at:
                    continue
                if await asyncio.to_thread(
                    self._repo.day_status, market, trade_date, KIND_TRADE_DATE
                ):
                    continue
                await self._verify_day(market, day, now_msk)

    async def _verify_day(self, market: str, day: date, now_msk: datetime) -> None:
        trade_date = day.isoformat()
        exchange = await self._history_numtrades(market, trade_date)
        if not exchange:
            if (now_msk.date() - day).days >= VERIFY_GIVE_UP_DAYS:
                await asyncio.to_thread(
                    self._repo.set_day_status,
                    market,
                    trade_date,
                    KIND_TRADE_DATE,
                    STATUS_UNVERIFIABLE,
                    "нет дневных итогов ISS",
                )
                logger.warning(
                    "ISS %s %s: дневных итогов нет — полноту не проверить", market, trade_date
                )
            return
        since = (day - timedelta(days=FEED_LOOKBACK_DAYS)).isoformat()
        ours = await asyncio.to_thread(
            self._repo.count_trades, market, trade_date, CHECK_BOARDS[market], since
        )
        mismatches = {
            secid: (ours.get(secid, 0), exchange.get(secid, 0))
            for secid in set(exchange) | set(ours)
            if ours.get(secid, 0) != exchange.get(secid, 0)
        }
        if not mismatches:
            status, detail = STATUS_COMPLETE, f"{len(exchange)} бумаг"
            logger.info(
                "ISS_DAY_VERIFIED %s %s: число сделок совпало по %d бумагам",
                market,
                trade_date,
                len(exchange),
            )
        else:
            # SPEC-0003 G-5: расхождение равносильно потере дня (G-2).
            status = STATUS_INCOMPLETE
            worst = sorted(mismatches.items(), key=lambda kv: -abs(kv[1][1] - kv[1][0]))[:10]
            detail = "; ".join(f"{s}: у нас {o}, биржа {e}" for s, (o, e) in worst)
            logger.error(
                "ISS_DAY_INCOMPLETE %s %s: расхождение по %d бумагам: %s",
                market,
                trade_date,
                len(mismatches),
                detail,
            )
        await asyncio.to_thread(
            self._repo.set_day_status, market, trade_date, KIND_TRADE_DATE, status, detail
        )

    async def _history_numtrades(self, market: str, trade_date: str) -> dict[str, int]:
        assert self._client is not None
        counts: dict[str, int] = {}
        start = 0
        while True:
            data = await self._client.get_json(
                HISTORY_URLS[market],
                {
                    "iss.meta": "off",
                    "iss.only": "history,history.cursor",
                    "date": trade_date,
                    "start": str(start),
                },
            )
            history = data.get("history") or {}
            columns = list(history.get("columns") or [])
            rows = list(history.get("data") or [])
            if not rows:
                break
            i_secid, i_num = columns.index("SECID"), columns.index("NUMTRADES")
            for row in rows:
                counts[str(row[i_secid])] = counts.get(str(row[i_secid]), 0) + int(row[i_num] or 0)
            cursor = ((data.get("history.cursor") or {}).get("data") or [[0, 0, 0]])[0]
            index, total, page = (int(v) for v in cursor)
            start = index + page
            if page <= 0 or start >= total:
                break
        return counts


def _read_raw_chunks(path: Path) -> Iterator[tuple[list[str], list[list[object]]]]:
    """Читает сырой CSV этапа 1 частями; бросает при обрезанном gzip."""
    with gzip.open(path, "rt", newline="") as fh:
        reader = csv.reader(fh)
        columns = next(reader, None)
        if columns is None:
            return
        chunk: list[list[object]] = []
        try:
            for row in reader:
                # Заголовок повторного члена gzip и оборванная строка не являются сделками.
                if row == columns or len(row) != len(columns):
                    continue
                chunk.append(list(row))
                if len(chunk) >= RAW_IMPORT_CHUNK:
                    yield columns, chunk
                    chunk = []
        except (EOFError, gzip.BadGzipFile, zlib.error, csv.Error):
            if chunk:
                yield columns, chunk  # прочитанное до обрыва не теряем
            raise
        if chunk:
            yield columns, chunk
