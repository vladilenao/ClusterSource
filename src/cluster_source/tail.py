"""Хвост вечерней сессии по минутным свечам ISS (SPEC-0011).

Бесплатная лента ISS отдаёт сделки с задержкой 15 минут, а в полночь МСК
окно задержки обрывается: последние минуты вечерней сессии (23:45–23:50 МСК)
в ленту не попадают никогда (исследование 2026-10-14). Хвост дня — минуты от
среза ленты до конца торгов — сверяется с минутными свечами ISS, которые
публикуются за весь день: объём (у акций — в штуках, наш объём × ``lot_size``),
максимум и минимум каждой минуты.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta

import pandas as pd

from cluster_source.iss import ISS_BASE, MSK, IssClient

logger = logging.getLogger(__name__)

CANDLE_URLS: dict[str, str] = {
    "shares": f"{ISS_BASE}/engines/stock/markets/shares/boards/TQBR/securities/{{secid}}"
    "/candles.json",
    "forts": f"{ISS_BASE}/engines/futures/markets/forts/securities/{{secid}}/candles.json",
}
#: Хвост — разрыв публикации ISS, только если он не длиннее 15 минут задержки с запасом.
TAIL_MAX = timedelta(minutes=20)
#: Свечей нет — повтор следующей ночью, пока дню меньше стольких суток.
TAIL_RETRY_DAYS = 3

TAIL_VERIFIED = "VERIFIED"
TAIL_MISMATCH = "MISMATCH"
TAIL_UNAVAILABLE = "UNAVAILABLE"
#: Вердикты минут хвоста в ``minute_verdicts``.
MINUTE_CANDLE_VERIFIED = "CANDLE_VERIFIED"
MINUTE_GAP = "GAP"

_EPS = 1e-9

Candle = tuple[float, float, float]  # объём, максимум, минимум
MinuteVerdict = tuple[str, str, int, int, int, str]


@dataclass(frozen=True)
class TailCheck:
    """Итог сверки хвоста дня: статус, наши сделки хвоста и вердикты минут."""

    status: str
    start: datetime
    n: int
    volume: float
    minutes: list[MinuteVerdict] = field(default_factory=list)
    bad: list[str] = field(default_factory=list)

    @property
    def label(self) -> str:
        """Пометка для справки дня (SPEC-0011 G-1)."""
        return (
            f"хвост по свечам с {self.start.astimezone(MSK):%H:%M} МСК:"
            f" {self.n} сделок / {self.volume:.0f}"
        )


async def fetch_candles(
    client: IssClient, market: str, secid: str, start: datetime
) -> dict[datetime, Candle]:
    """Минутные свечи бумаги от ``start`` до конца её календарного дня МСК."""
    begin = start.astimezone(MSK)
    data = await client.get_json(
        CANDLE_URLS[market].format(secid=secid),
        {
            "iss.meta": "off",
            "iss.only": "candles",
            "interval": "1",
            "from": begin.strftime("%Y-%m-%d %H:%M"),
            "till": begin.strftime("%Y-%m-%d 23:59"),
        },
    )
    block = data.get("candles") or {}
    columns = list(block.get("columns") or [])
    out: dict[datetime, Candle] = {}
    if not columns:
        return out
    i_begin, i_vol, i_high, i_low = (columns.index(c) for c in ("begin", "volume", "high", "low"))
    for row in block.get("data") or []:
        moment = datetime.fromisoformat(str(row[i_begin])).replace(tzinfo=MSK).astimezone(UTC)
        out[moment] = (float(row[i_vol] or 0), float(row[i_high]), float(row[i_low]))
    return out


def _close(a: float, b: float) -> bool:
    return abs(a - b) <= _EPS * max(1.0, abs(b))


def compare_tail(
    ticks: pd.DataFrame, candles: dict[datetime, Candle], start: datetime, lot: int
) -> TailCheck:
    """Сверяет наши сделки хвоста (``timestamp`` ≥ ``start``) со свечами по минутам."""
    tail = ticks.loc[ticks["timestamp"] >= pd.Timestamp(start)]
    ours: dict[datetime, tuple[int, float, float, float]] = {}
    if not tail.empty:
        grouped = tail.groupby(tail["timestamp"].dt.floor("min"))
        for minute, g in grouped:
            ours[pd.Timestamp(minute).to_pydatetime()] = (
                len(g),
                float(g["volume"].sum()),
                float(g["price"].max()),
                float(g["price"].min()),
            )
    minutes: list[MinuteVerdict] = []
    bad: list[str] = []
    for minute in sorted(set(ours) | {m for m in candles if m >= start}):
        n, volume, high, low = ours.get(minute, (0, 0.0, 0.0, 0.0))
        candle = candles.get(minute)
        ok = (
            candle is not None
            and _close(volume * lot, candle[0])
            and _close(high, candle[1])
            and _close(low, candle[2])
        )
        if ok:
            minutes.append((minute.isoformat(), MINUTE_CANDLE_VERIFIED, n, 0, 0, "candle"))
            continue
        theirs = (
            "нет свечи" if candle is None else f"свеча {candle[0]:g}/{candle[1]:g}/{candle[2]:g}"
        )
        minutes.append(
            (
                minute.isoformat(),
                MINUTE_GAP,
                n,
                0,
                0,
                f"candle_mismatch: у нас {volume * lot:g}/{high:g}/{low:g}, {theirs}",
            )
        )
        bad.append(f"{minute.astimezone(MSK):%H:%M}")
    return TailCheck(
        TAIL_MISMATCH if bad else TAIL_VERIFIED,
        start,
        int(len(tail)),
        float(tail["volume"].sum()) if not tail.empty else 0.0,
        minutes,
        bad,
    )


async def check_tail(
    client: IssClient,
    market: str,
    secid: str,
    ticks: pd.DataFrame,
    start: datetime,
    lot: int,
) -> TailCheck:
    """Хвост дня против свечей ISS; свечей нет при наших сделках — ``UNAVAILABLE``."""
    candles = await fetch_candles(client, market, secid, start)
    has_tail = bool((ticks["timestamp"] >= pd.Timestamp(start)).any()) if not ticks.empty else False
    if not candles and has_tail:
        return TailCheck(TAIL_UNAVAILABLE, start, 0, 0.0)
    return compare_tail(ticks, candles, start, lot)
