"""Сверка стрима T-Invest с лентой Мосбиржи и дозаливка (SPEC-0005).

Раз в минуту для каждого инструмента сверяются целые минуты от курсора до
границы ленты ISS (последняя сделка текущей ленты рынка − 2 с, но не позже
«сейчас − 16 мин», когда консьюмер дописал тики минуты). В каждой минуте
сделки сравниваются посекундно как мультимножества (цена, количество, сторона):

* совпали — секунда ``VERIFIED``, сделкам стрима проставляется ``TRADENO``;
* у нас меньше (наши — подмножество биржевых) — секунда заменяется сделками
  ISS (``origin = iss``), минута ``REPAIRED``;
* у нас есть сделка, которой нет у ISS, или у ISS пусто при наших сделках —
  минута ``GAP``, данные не трогаются: удалять наши данные по слову ISS при
  сомнении в покрытии нельзя.

Исследование 2026-10-13 (56 инструментов): при одном соединении стрим и ISS
совпали на 100% секунд, сдвиг времени нулевой, «лишних» сделок у нас не было.
"""

from __future__ import annotations

import asyncio
import logging
from collections import Counter, defaultdict
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta

from cluster_source.config import AppConfig, Instrument, InstrumentType
from cluster_source.database import IssTapeRepository, MarketDataRepository
from cluster_source.quality import minute_floor

logger = logging.getLogger(__name__)

RECONCILE_INTERVAL_SEC = 60.0
#: Граница рынка — последняя сделка текущей ленты ISS минус запас.
ISS_EDGE = timedelta(seconds=2)
#: Минута сверяется не раньше, чем консьюмер точно дописал её тики.
SETTLE = timedelta(minutes=16)
#: Не больше стольких минут на инструмент за проход (первый проход за день — с утра).
MAX_MINUTES_PER_RUN = 720

STATUS_VERIFIED = "VERIFIED"
STATUS_REPAIRED = "REPAIRED"
STATUS_GAP = "GAP"

BOARDS = {"shares": "TQBR", "forts": "RFUD"}

StreamRow = tuple[int, str, float, int, str]  # rowid, время, цена, объём, сторона
IssRow = tuple[int, str, float, int, str]  # TRADENO, время, цена, количество, сторона
Key = tuple[float, int, str]


def market_of(instrument: Instrument) -> str:
    return "shares" if instrument.type == InstrumentType.SHARE else "forts"


def _key(price: float, qty: int, direction: str) -> Key:
    return (round(price, 6), int(qty), direction)


@dataclass
class MinuteResult:
    """Вердикт минуты и действия с данными."""

    status: str
    stream_n: int
    iss_n: int
    repaired: int = 0
    detail: str = ""
    assignments: list[tuple[int, int]] = field(default_factory=list)
    delete_rowids: list[int] = field(default_factory=list)
    new_rows: list[dict[str, object]] = field(default_factory=list)


def reconcile_minute(
    storage_name: str, stream_rows: list[StreamRow], iss_rows: list[IssRow]
) -> MinuteResult | None:
    """Сверяет одну минуту. ``None`` — сделок нет ни у нас, ни у ISS."""
    if not stream_rows and not iss_rows:
        return None
    if not iss_rows:
        return MinuteResult(STATUS_GAP, len(stream_rows), 0, detail="iss_empty")

    ours: dict[str, list[StreamRow]] = defaultdict(list)
    theirs: dict[str, list[IssRow]] = defaultdict(list)
    for row in stream_rows:
        ours[row[1][:19]].append(row)
    for row in iss_rows:
        theirs[row[1][:19]].append(row)

    result = MinuteResult(STATUS_VERIFIED, len(stream_rows), len(iss_rows))
    extra_secs: list[str] = []
    side_only_secs = 0
    for sec in sorted(set(ours) | set(theirs)):
        s_rows, i_rows = ours.get(sec, []), theirs.get(sec, [])
        s_keys = Counter(_key(r[2], r[3], r[4]) for r in s_rows)
        i_keys = Counter(_key(r[2], r[3], r[4]) for r in i_rows)
        if s_keys == i_keys:
            pool: dict[Key, list[int]] = defaultdict(list)
            for tradeno, _ts, price, qty, side in i_rows:
                pool[_key(price, qty, side)].append(tradeno)
            for rowid, _ts, price, qty, side in s_rows:
                result.assignments.append((rowid, pool[_key(price, qty, side)].pop()))
            continue
        if not s_keys - i_keys:  # наши сделки — подмножество биржевых: дозаливка
            result.repaired += 1
            result.delete_rowids.extend(r[0] for r in s_rows)
            moment = datetime.fromisoformat(sec).replace(tzinfo=UTC)
            for tradeno, _ts, price, qty, side in i_rows:
                result.new_rows.append(
                    {
                        "ticker": storage_name,
                        "timestamp": moment,
                        "price": price,
                        "volume": qty,
                        "direction": side,
                        "trade_id": f"x{tradeno}",
                        "origin": "iss",
                        "exchange_trade_no": tradeno,
                    }
                )
            continue
        extra_secs.append(sec)
        if Counter((k[0], k[1]) for k in s_keys.elements()) == Counter(
            (k[0], k[1]) for k in i_keys.elements()
        ):
            side_only_secs += 1

    if extra_secs:
        detail = "direction_mismatch" if side_only_secs * 2 > len(extra_secs) else "extra_trades"
        return MinuteResult(
            STATUS_GAP,
            len(stream_rows),
            len(iss_rows),
            detail=f"{detail}: {len(extra_secs)} с, первая {extra_secs[0][11:]}",
        )
    if result.repaired:
        result.status = STATUS_REPAIRED
        result.detail = f"дозалито {len(result.new_rows) - len(result.delete_rowids)} сделок"
    return result


class Reconciler:
    """Фоновая сверка горячего хранилища с лентой ISS (SPEC-0005)."""

    def __init__(
        self,
        config: AppConfig,
        repos: dict[str, MarketDataRepository],
        iss_repo: IssTapeRepository,
        ticker_of: Callable[[Instrument], str | None],
        *,
        is_blocked: Callable[[str], bool] = lambda _name: False,
        now: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        self._config = config
        self._repos = repos
        self._iss = iss_repo
        self._ticker_of = ticker_of
        self._is_blocked = is_blocked
        self._now = now

    async def run(self) -> None:
        while True:
            await self.run_once()
            await asyncio.sleep(RECONCILE_INTERVAL_SEC)

    async def run_once(self) -> None:
        for instrument in self._config.instruments:
            try:
                await asyncio.to_thread(self.reconcile_instrument, instrument)
            except Exception as exc:  # noqa: BLE001 - один инструмент не роняет сверку
                logger.error("Ошибка сверки %s: %s", instrument.id, exc)

    def reconcile_instrument(self, instrument: Instrument) -> Counter[str]:
        """Сверяет готовые минуты инструмента; возвращает счётчик вердиктов."""
        stats: Counter[str] = Counter()
        name = instrument.storage_name
        ticker = self._ticker_of(instrument)
        if ticker is None or self._is_blocked(name):
            return stats
        market = market_of(instrument)
        bounds = self._iss.feed_bounds(market)
        if bounds is None:
            return stats
        feed_start, feed_last = bounds
        end_minute = minute_floor(min(feed_last - ISS_EDGE, self._now() - SETTLE))
        repo = self._repos[name]
        stored = repo.verified_until()
        cursor = datetime.fromisoformat(stored) if stored else minute_floor(feed_start)
        if cursor >= end_minute:
            return stats
        end = min(end_minute, cursor + timedelta(minutes=MAX_MINUTES_PER_RUN))

        ours: dict[str, list[StreamRow]] = defaultdict(list)
        theirs: dict[str, list[IssRow]] = defaultdict(list)
        for row in repo.read_ticks_for_reconcile(cursor, end):
            ours[row[1][:16]].append(row)
        seen: set[int] = set()
        for iss_row in self._iss.read_trades(market, ticker, BOARDS[market], cursor, end):
            if iss_row[0] not in seen:  # дубли SQLite/Parquet после падения
                seen.add(iss_row[0])
                theirs[iss_row[1][:16]].append(iss_row)

        added = 0
        minute = cursor
        while minute < end:
            key = minute.isoformat()[:16]
            nxt = minute + timedelta(minutes=1)
            day_archived = repo.parquet_day_path(minute.strftime("%Y-%m-%d")) is not None
            result = None if day_archived else reconcile_minute(name, ours[key], theirs[key])
            if result is not None:
                repo.apply_minute_verdict(
                    minute.isoformat(),
                    (result.status, result.stream_n, result.iss_n, result.repaired, result.detail),
                    result.assignments,
                    result.delete_rowids,
                    result.new_rows,
                    nxt.isoformat(),
                )
                stats[result.status] += 1
                added += len(result.new_rows) - len(result.delete_rowids)
                if "direction_mismatch" in result.detail:
                    logger.error(
                        "ISS_DIRECTION_MISMATCH %s минута %s: %s", name, key, result.detail
                    )
            minute = nxt
        repo.apply_minute_verdict(end.isoformat(), ("", 0, 0, 0, ""), [], [], [], end.isoformat())

        if stats:
            level = logging.WARNING if stats[STATUS_GAP] or stats[STATUS_REPAIRED] else logging.INFO
            logger.log(
                level,
                "RECONCILE %s: минут %d (VERIFIED %d, REPAIRED %d, GAP %d),"
                " дозалито %d сделок, до %s",
                name,
                sum(stats.values()),
                stats[STATUS_VERIFIED],
                stats[STATUS_REPAIRED],
                stats[STATUS_GAP],
                added,
                end.isoformat(),
            )
        return stats
