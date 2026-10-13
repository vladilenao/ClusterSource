"""Ночной статус FINAL: торговый день против эталона биржи (SPEC-0006).

Эталон акций — дневные итоги ISS (``NUMTRADES``; ``VOLUME`` в штуках = наш
объём в лотах × ``lot_size``). Эталон фьючерсов — полная лента ISS за
торговый день: в дневные итоги FORTS входят крупные блочные/переговорные
сделки, которых нет ни в одной анонимной ленте (исследование 2026-10-13:
медиана 0,05% сделок, до 15% объёма), поэтому точного совпадения с итогами
для фьючерсов не бывает.

Торговый день D — дата с итогами ISS; календарные дни после предыдущего
торгового дня до D включительно (выходные, праздники) относятся к D.
"""

from __future__ import annotations

import asyncio
import logging
from collections import Counter
from collections.abc import Mapping
from datetime import date, timedelta
from typing import Any

from cluster_source.autoload import REFRESH_FINAL_MISMATCH
from cluster_source.config import (
    AppConfig,
    FuturesMeta,
    Instrument,
    InstrumentType,
    nearest_contract,
)
from cluster_source.database import IssTapeRepository, MarketDataRepository
from cluster_source.iss import HISTORY_URLS, KIND_FEED, STATUS_COMPLETE, IssClient

logger = logging.getLogger(__name__)

STATUS_FINAL = "FINAL"
STATUS_MISMATCH = "MISMATCH"
STATUS_UNVERIFIABLE = "UNVERIFIABLE"
#: Фьючерсы без полной ленты ISS: день согласуется с итогами биржи в пределах
#: известного разрыва (сделки вне анонимных лент), но не доказан (решение 2026-10-13).
STATUS_ARCHIVE_CONSISTENT = "ARCHIVE_CONSISTENT"
#: Статусы, которые повторно не проверяются.
SETTLED = frozenset({STATUS_FINAL, STATUS_ARCHIVE_CONSISTENT})
#: Максимальная доля сделок FORTS вне анонимной ленты (исследование: до 0,76%).
FORTS_OUTSIDE_TAPE_MAX_SHARE = 0.01
#: Префикс ``detail`` дня, который перекачка из архива не изменила (G-5).
ARCHIVE_EXHAUSTED = "archive_exhausted"

BOARDS = {"shares": "TQBR", "forts": "RFUD"}
#: Итоги за дату точно опубликованы через столько суток: пустой ответ — неторговая дата.
TOTALS_PUBLISHED_AFTER_DAYS = 3
_VOLUME_EPS = 1e-6


def market_of(instrument: Instrument) -> str:
    return "shares" if instrument.type == InstrumentType.SHARE else "forts"


async def fetch_totals(
    client: IssClient, market: str, trade_date: str
) -> dict[str, tuple[int, float]]:
    """Дневные итоги рынка ISS за дату: ``{SECID: (NUMTRADES, VOLUME)}`` (постранично)."""
    totals: dict[str, tuple[int, float]] = {}
    start = 0
    while True:
        data = await client.get_json(
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
            return totals
        i_secid, i_num, i_vol = (columns.index(c) for c in ("SECID", "NUMTRADES", "VOLUME"))
        for row in rows:
            secid = str(row[i_secid])
            n, v = totals.get(secid, (0, 0.0))
            totals[secid] = (n + int(row[i_num] or 0), v + float(row[i_vol] or 0))
        cursor = ((data.get("history.cursor") or {}).get("data") or [[0, 0, 0]])[0]
        index, total, page = (int(x) for x in cursor)
        start = index + page
        if page <= 0 or start >= total:
            return totals


async def _totals(
    client: IssClient, iss: IssTapeRepository, market: str, day: date, today: date
) -> dict[str, tuple[int, float]]:
    cached = await asyncio.to_thread(iss.cached_totals, market, day.isoformat())
    if cached is not None:
        return cached
    totals = await fetch_totals(client, market, day.isoformat())
    # Пустой ответ кэшируем как «неторговая дата», только когда итоги точно вышли.
    if totals or (today - day).days >= TOTALS_PUBLISHED_AFTER_DAYS:
        await asyncio.to_thread(iss.save_totals, market, day.isoformat(), totals)
    return totals


def trading_spans(days: list[date], trading: set[date]) -> dict[date, list[date]]:
    """Торговый день → его календарные дни (выходные и праздники — к следующему).

    Дни после последнего торгового дня ни к чему не относятся: их итоги ещё
    могут не выйти.
    """
    spans: dict[date, list[date]] = {}
    pending: list[date] = []
    for day in sorted(days):
        pending.append(day)
        if day in trading:
            spans[day] = pending
            pending = []
    return spans


def verdict(ours: tuple[int, float], ref: tuple[int, float]) -> str:
    same = ours[0] == ref[0] and abs(ours[1] - ref[1]) <= _VOLUME_EPS * max(1.0, ref[1])
    return STATUS_FINAL if same else STATUS_MISMATCH


async def run_final_check(
    config: AppConfig,
    repos: Mapping[str, MarketDataRepository],
    iss: IssTapeRepository,
    client: IssClient,
    futures: Mapping[str, FuturesMeta],
    today: date,
) -> Counter[str]:
    """Выносит вердикт по всем непроверенным прошедшим торговым дням (SPEC-0006)."""
    stats: Counter[str] = Counter()
    for market in ("shares", "forts"):
        instruments = [i for i in config.instruments if market_of(i) == market]
        stored: set[date] = set()
        for inst in instruments:
            stored |= {
                date.fromisoformat(d)
                for d in await asyncio.to_thread(repos[inst.storage_name].available_days)
            }
        stored = {d for d in stored if d < today}
        if not stored:
            continue
        days = [min(stored) + timedelta(days=k) for k in range((today - min(stored)).days)]
        trading = {d for d in days if await _totals(client, iss, market, d, today)}
        spans = trading_spans(days, trading)
        for inst in instruments:
            stats += await _check_instrument(
                inst, repos[inst.storage_name], iss, client, futures, spans, today
            )
    return stats


async def _check_instrument(
    inst: Instrument,
    repo: MarketDataRepository,
    iss: IssTapeRepository,
    client: IssClient,
    futures: Mapping[str, FuturesMeta],
    spans: dict[date, list[date]],
    today: date,
) -> Counter[str]:
    stats: Counter[str] = Counter()
    market = market_of(inst)
    done = await asyncio.to_thread(repo.day_verdicts)
    final_days = set(done.loc[done["status"].isin(SETTLED), "trade_date"])
    previous = {str(row.trade_date): row for row in done.itertuples(index=False)}
    marks = await asyncio.to_thread(repo.days_to_refresh)
    for trade_date, span in spans.items():
        if trade_date.isoformat() in final_days:
            continue
        n, volume = 0, 0.0
        for day in span:
            dn, dv = await asyncio.to_thread(repo.day_totals, day.isoformat())
            n, volume = n + dn, volume + dv
        status: str | None
        if market == "shares":
            totals = await _totals(client, iss, market, trade_date, today)
            ref = totals.get(inst.id, (0, 0.0))
            ours = (n, volume * inst.lot_size)
            status = None if n == 0 and ref[0] == 0 else verdict(ours, ref)
            reference, detail = "iss_totals", ""
        else:
            ours = (n, volume)
            status, ref, reference, detail = await _forts_verdict(
                inst, iss, client, futures, trade_date, ours, today
            )
        if status is None:
            continue
        span_days = [d.isoformat() for d in span]
        exhausted = False
        prev = previous.get(trade_date.isoformat())
        if status == STATUS_MISMATCH and prev is not None and _same_verdict(prev, ours, ref):
            if str(prev.detail).startswith(ARCHIVE_EXHAUSTED):
                continue  # SPEC-0006 G-5: уже признан исчерпанным, данные не менялись
            # Перекачка прошла (отметок нет) и ничего не изменила (SPEC-0006 G-5).
            exhausted = not any(day in marks for day in span_days)
            if exhausted:
                detail = f"{ARCHIVE_EXHAUSTED}: перекачка из архива не изменила день; {detail}"
        await asyncio.to_thread(
            repo.set_day_verdict, trade_date.isoformat(), status, ours, ref, reference, detail
        )
        stats[status] += 1
        if status in SETTLED:  # перекачка из архива ничего не улучшит
            for day_str in span_days:
                await asyncio.to_thread(repo.clear_refresh_mark, day_str)
        elif exhausted:
            logger.error(
                "DAY_ARCHIVE_EXHAUSTED %s %s: перекачка из архива не изменила день"
                " (у нас %d сделок / %.0f, эталон %d / %.0f) — остаётся MISMATCH,"
                " больше не перекачивается",
                inst.storage_name,
                trade_date,
                ours[0],
                ours[1],
                ref[0],
                ref[1],
            )
        elif status == STATUS_MISMATCH:
            logger.error(
                "DAY_NOT_FINAL %s %s: у нас %d сделок / %.0f, эталон %d / %.0f (%s)",
                inst.storage_name,
                trade_date,
                ours[0],
                ours[1],
                ref[0],
                ref[1],
                reference,
            )
            await asyncio.to_thread(repo.mark_days_for_refresh, span_days, REFRESH_FINAL_MISMATCH)
    if stats:
        level = logging.WARNING if stats[STATUS_MISMATCH] else logging.INFO
        logger.log(
            level,
            "FINAL %s: торговых дней %d (FINAL %d, ARCHIVE_CONSISTENT %d, MISMATCH %d,"
            " UNVERIFIABLE %d)",
            inst.storage_name,
            sum(stats.values()),
            stats[STATUS_FINAL],
            stats[STATUS_ARCHIVE_CONSISTENT],
            stats[STATUS_MISMATCH],
            stats[STATUS_UNVERIFIABLE],
        )
    return stats


def _same_verdict(prev: Any, ours: tuple[int, float], ref: tuple[int, float]) -> bool:
    """Прошлый вердикт дня — ``MISMATCH`` с теми же числами (наши и эталон)."""

    def close(a: float, b: float) -> bool:
        return abs(a - b) <= _VOLUME_EPS * max(1.0, abs(b))

    return (
        prev.status == STATUS_MISMATCH
        and int(prev.ours_n) == ours[0]
        and int(prev.ref_n) == ref[0]
        and close(float(prev.ours_volume), ours[1])
        and close(float(prev.ref_volume), ref[1])
    )


async def _forts_verdict(
    inst: Instrument,
    iss: IssTapeRepository,
    client: IssClient,
    futures: Mapping[str, FuturesMeta],
    trade_date: date,
    ours: tuple[int, float],
    today: date,
) -> tuple[str | None, tuple[int, float], str, str]:
    """Фьючерсы: эталон — полная лента ISS за торговый день; итоги — справкой.

    Без полной ленты: согласие с итогами в пределах сделок вне анонимной ленты —
    ``ARCHIVE_CONSISTENT``; больше расхождение — ``MISMATCH`` (перекачка); итогов
    нет — ``UNVERIFIABLE``.
    """
    if inst.is_continuous:
        meta = nearest_contract(inst, futures.values(), trade_date)
        secid = meta.ticker if meta is not None else None
    else:
        secid = inst.id
    if secid is None:
        return STATUS_UNVERIFIABLE, (0, 0.0), "none", "не определён контракт на дату"
    totals = await _totals(client, iss, "forts", trade_date, today)
    official = totals.get(secid, (0, 0.0))
    if ours[0] == 0 and official[0] == 0:
        return None, (0, 0.0), "", ""
    feed_status = await asyncio.to_thread(
        iss.day_status, "forts", trade_date.isoformat(), KIND_FEED
    )
    tape = await asyncio.to_thread(
        iss.tape_day, "forts", secid, BOARDS["forts"], trade_date.isoformat()
    )
    outside = (
        f"{secid}; вне анонимной ленты: {official[0] - tape[0]} сделок"
        f" / {official[1] - tape[1]:.0f}"
    )
    if feed_status == STATUS_COMPLETE and tape[0] > 0:
        return verdict(ours, tape), tape, "iss_tape", outside
    if official[0] == 0:
        return STATUS_UNVERIFIABLE, official, "none", f"{secid}: нет итогов ISS за день"
    deficit = official[0] - ours[0]
    consistent = 0 <= deficit <= FORTS_OUTSIDE_TAPE_MAX_SHARE * official[0] and ours[1] <= official[
        1
    ] * (1 + _VOLUME_EPS)
    status = STATUS_ARCHIVE_CONSISTENT if consistent else STATUS_MISMATCH
    detail = f"{secid}: нет полной ленты ISS; итоги {official[0]} сделок, у нас на {deficit} меньше"
    return status, official, "iss_totals", detail
