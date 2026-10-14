"""Ночной статус FINAL: торговый день против эталона биржи (SPEC-0006).

Эталон акций — дневные итоги ISS (``NUMTRADES``; ``VOLUME`` в штуках = наш
объём в лотах × ``lot_size``). Эталон фьючерсов — полная лента ISS за
торговый день: в дневные итоги FORTS входят крупные блочные/переговорные
сделки, которых нет ни в одной анонимной ленте (исследование 2026-10-13:
медиана 0,05% сделок, до 15% объёма), поэтому точного совпадения с итогами
для фьючерсов не бывает.

Торговый день D — дата с итогами ISS; календарные дни после предыдущего
торгового дня до D включительно (выходные, праздники) относятся к D.

Последние минуты вечерней сессии бесплатный ISS не публикует (SPEC-0011):
голова дня сверяется с лентой, хвост — с минутными свечами ISS.
"""

from __future__ import annotations

import asyncio
import logging
from collections import Counter
from collections.abc import Mapping
from datetime import date, datetime, timedelta
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
from cluster_source.iss import HISTORY_URLS, KIND_FEED, MSK, STATUS_INCOMPLETE, IssClient
from cluster_source.quality import minute_floor
from cluster_source.tail import (
    TAIL_MAX,
    TAIL_RETRY_DAYS,
    TAIL_UNAVAILABLE,
    TAIL_VERIFIED,
    TailCheck,
    check_tail,
)

logger = logging.getLogger(__name__)

STATUS_FINAL = "FINAL"
STATUS_MISMATCH = "MISMATCH"
STATUS_UNVERIFIABLE = "UNVERIFIABLE"
#: Фьючерсы без полной ленты ISS: день согласуется с итогами биржи в пределах
#: известного разрыва (сделки вне анонимных лент), но не доказан (решение 2026-10-13).
STATUS_ARCHIVE_CONSISTENT = "ARCHIVE_CONSISTENT"
#: Статусы, которые снимают отметки перекачки (архив ничего не улучшит).
SETTLED = frozenset({STATUS_FINAL, STATUS_ARCHIVE_CONSISTENT})
#: ``ARCHIVE_CONSISTENT`` моложе стольких суток перепроверяется: лента и свечи
#: дня могли стать доступны позже (SPEC-0011).
ARCHIVE_RECHECK_DAYS = 3
#: Максимальная доля сделок FORTS вне анонимной ленты (исследование: до 0,76%).
FORTS_OUTSIDE_TAPE_MAX_SHARE = 0.01
#: Префикс ``detail`` дня, который перекачка из архива не изменила (G-5).
ARCHIVE_EXHAUSTED = "archive_exhausted"

BOARDS = {"shares": "TQBR", "forts": "RFUD"}
#: Итоги за дату точно опубликованы через столько суток: пустой ответ — неторговая дата.
TOTALS_PUBLISHED_AFTER_DAYS = 3
_VOLUME_EPS = 1e-6
Cutoffs = dict[tuple[str, str], datetime | None]


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
    cutoffs: Cutoffs = {}  # срез ленты на рынок и день — один раз за проход
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
                inst, repos[inst.storage_name], iss, client, futures, spans, today, cutoffs
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
    cutoffs: Cutoffs,
) -> Counter[str]:
    stats: Counter[str] = Counter()
    market = market_of(inst)
    done = await asyncio.to_thread(repo.day_verdicts)
    final_days = {
        str(row.trade_date)
        for row in done.itertuples(index=False)
        if row.status == STATUS_FINAL
        or (
            row.status == STATUS_ARCHIVE_CONSISTENT
            and (today - date.fromisoformat(str(row.trade_date))).days >= ARCHIVE_RECHECK_DAYS
        )
    }
    previous = {str(row.trade_date): row for row in done.itertuples(index=False)}
    marks = await asyncio.to_thread(repo.days_to_refresh)
    for trade_date, span in spans.items():
        n, volume = 0, 0.0
        for day in span:
            dn, dv = await asyncio.to_thread(repo.day_totals, day.isoformat())
            n, volume = n + dn, volume + dv
        if trade_date.isoformat() in final_days:
            prev_row: Any = previous[trade_date.isoformat()]
            stored_n, stored_volume = int(prev_row.ours_n), float(prev_row.ours_volume)
            now_volume = volume * inst.lot_size if market == "shares" else volume
            if stored_n == n and _close(stored_volume, now_volume):
                continue
            # SPEC-0006 G-6: данные дня изменились (перекачка, дозаливка) — вердикт устарел.
            logger.info(
                "DAY_DATA_CHANGED %s %s: в вердикте %d сделок, сейчас %d — проверяю заново",
                inst.storage_name,
                trade_date,
                stored_n,
                n,
            )
        status: str | None
        if market == "shares":
            totals = await _totals(client, iss, market, trade_date, today)
            ref = totals.get(inst.id, (0, 0.0))
            ours = (n, volume * inst.lot_size)
            status = None if n == 0 and ref[0] == 0 else verdict(ours, ref)
            reference, detail = "iss_totals", ""
            if status is not None:  # SPEC-0011: хвост — для минутных вердиктов и справки
                tail = await _tail(
                    inst, repo, iss, client, market, inst.id, trade_date, span, cutoffs
                )
                if tail is not None and tail.status != TAIL_UNAVAILABLE:
                    detail = tail.label
        else:
            ours = (n, volume)
            status, ref, reference, detail = await _forts_verdict(
                inst, repo, iss, client, futures, trade_date, span, ours, today, cutoffs
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


def _close(a: float, b: float) -> bool:
    return abs(a - b) <= _VOLUME_EPS * max(1.0, abs(b))


def _same_verdict(prev: Any, ours: tuple[int, float], ref: tuple[int, float]) -> bool:
    """Прошлый вердикт дня — ``MISMATCH`` с теми же числами (наши и эталон)."""
    return (
        prev.status == STATUS_MISMATCH
        and int(prev.ours_n) == ours[0]
        and int(prev.ref_n) == ref[0]
        and _close(float(prev.ours_volume), ours[1])
        and _close(float(prev.ref_volume), ref[1])
    )


async def _tail(
    inst: Instrument,
    repo: MarketDataRepository,
    iss: IssTapeRepository,
    client: IssClient,
    market: str,
    secid: str,
    trade_date: date,
    span: list[date],
    cutoffs: Cutoffs,
) -> TailCheck | None:
    """Сверяет хвост дня со свечами ISS и пишет вердикты его минут (SPEC-0011).

    ``None`` — у дня нет ленты или хвост длиннее :data:`TAIL_MAX`: тогда лента
    не покрывает день, и свечами его не подтвердить (SPEC-0011 G-2).
    """
    key = (market, trade_date.isoformat())
    if key not in cutoffs:
        cutoffs[key] = await asyncio.to_thread(iss.tape_cutoff, *key)
    cutoff = cutoffs[key]
    if cutoff is None:
        return None
    start = minute_floor(cutoff)
    ticks = await asyncio.to_thread(repo.read_day, span[-1].isoformat())
    if not ticks.empty and ticks["timestamp"].max() - start > TAIL_MAX:
        return None  # SPEC-0011 G-2: не разрыв публикации, а отставание ленты
    lot = inst.lot_size if market == "shares" else 1
    tail = await check_tail(client, market, secid, ticks, start, lot)
    name = inst.storage_name
    if tail.status == TAIL_UNAVAILABLE:
        logger.warning(
            "TAIL_UNAVAILABLE %s %s: свечей ISS нет — повтор следующей ночью", name, trade_date
        )
        return tail
    await asyncio.to_thread(repo.save_minute_verdicts, tail.minutes)
    if tail.status == TAIL_VERIFIED:
        logger.info(
            "TAIL %s %s: хвост с %s МСК, минут %d — совпал со свечами",
            name,
            trade_date,
            f"{tail.start.astimezone(MSK):%H:%M}",
            len(tail.minutes),
        )
    else:
        logger.error("TAIL_MISMATCH %s %s: минуты %s", name, trade_date, ", ".join(tail.bad))
    return tail


async def _forts_verdict(
    inst: Instrument,
    repo: MarketDataRepository,
    iss: IssTapeRepository,
    client: IssClient,
    futures: Mapping[str, FuturesMeta],
    trade_date: date,
    span: list[date],
    ours: tuple[int, float],
    today: date,
    cutoffs: Cutoffs,
) -> tuple[str | None, tuple[int, float], str, str]:
    """Фьючерсы: эталон — лента ISS за торговый день, хвост — свечи; итоги — справкой.

    Голова дня (до среза ленты) сверяется с лентой сделка в сделку, хвост —
    с минутными свечами (SPEC-0011). Без ленты, покрывающей день: согласие с
    итогами в пределах сделок вне анонимной ленты — ``ARCHIVE_CONSISTENT``;
    больше расхождение — ``MISMATCH`` (перекачка); итогов нет — ``UNVERIFIABLE``.
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
    if tape[0] > 0 and feed_status != STATUS_INCOMPLETE:
        tail = await _tail(inst, repo, iss, client, "forts", secid, trade_date, span, cutoffs)
        if tail is not None and tail.status == TAIL_UNAVAILABLE:
            if (today - trade_date).days < TAIL_RETRY_DAYS:
                return None, (0, 0.0), "", ""  # SPEC-0011 G-3: повтор следующей ночью
        elif tail is not None:
            head_ref = await asyncio.to_thread(
                iss.tape_day, "forts", secid, BOARDS["forts"], trade_date.isoformat(), tail.start
            )
            head_ours = (ours[0] - tail.n, ours[1] - tail.volume)
            ref = (head_ref[0] + tail.n, head_ref[1] + tail.volume)
            head_ok = verdict(head_ours, head_ref) == STATUS_FINAL
            detail = f"{tail.label}; {outside}"
            if not head_ok:
                detail += f"; до среза у нас {head_ours[0]} сделок, в ленте {head_ref[0]}"
            # SPEC-0011 G-1: голова — сделка в сделку по ленте, хвост — по свечам.
            ok = head_ok and tail.status == TAIL_VERIFIED
            return (STATUS_FINAL if ok else STATUS_MISMATCH), ref, "iss_tape", detail
    if official[0] == 0:
        return STATUS_UNVERIFIABLE, official, "none", f"{secid}: нет итогов ISS за день"
    deficit = official[0] - ours[0]
    consistent = 0 <= deficit <= FORTS_OUTSIDE_TAPE_MAX_SHARE * official[0] and ours[1] <= official[
        1
    ] * (1 + _VOLUME_EPS)
    status = STATUS_ARCHIVE_CONSISTENT if consistent else STATUS_MISMATCH
    detail = f"{secid}: нет полной ленты ISS; итоги {official[0]} сделок, у нас на {deficit} меньше"
    return status, official, "iss_totals", detail
