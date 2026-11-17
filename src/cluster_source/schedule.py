"""Расписание торгов (SPEC-0012), этап 1 — календарь торговых дней ISS.

Ни T-Invest, ни ISS не отдают историю времени сессий, зато ISS отдаёт
календарь рынков (``/iss/engines/{stock,futures}.json``): недельный шаблон
``timetable`` (все 7 дней торговые — с сессиями выходного дня) и исключения по
датам ``dailytable`` с 2018 года — праздники, отменённые сессии выходного дня,
рабочие дни в праздники. Времена в нём — часы торговой системы, не сессий.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Iterable, Mapping
from datetime import UTC, date, datetime, time, timedelta
from typing import Any

import numpy as np
import pandas as pd

from cluster_source.autoload import REFRESH_CALENDAR_GAP
from cluster_source.config import AppConfig, Instrument, InstrumentType, SignalsConfig
from cluster_source.database import IssTapeRepository, MarketDataRepository
from cluster_source.iss import ISS_BASE, MSK, IssClient

logger = logging.getLogger(__name__)

ENGINE_URLS: dict[str, str] = {
    "shares": f"{ISS_BASE}/engines/stock.json",
    "forts": f"{ISS_BASE}/engines/futures.json",
}


def _rows(data: dict[str, object], block: str) -> list[dict[str, object]]:
    raw = data.get(block)
    if not isinstance(raw, dict):
        return []
    columns = list(raw.get("columns") or [])
    return [dict(zip(columns, row, strict=False)) for row in raw.get("data") or []]


def _opt(value: object) -> str | None:
    return None if value in (None, "") else str(value)


async def load_iss_calendar(client: IssClient, repo: IssTapeRepository) -> dict[str, int]:
    """Загружает календарь обоих рынков; ``{рынок: число дат-исключений}``.

    Ошибка одного рынка логируется, прежний календарь остаётся (SPEC-0012 G-2).
    """
    loaded: dict[str, int] = {}
    now = datetime.now(UTC).isoformat()
    for market, url in ENGINE_URLS.items():
        try:
            data = await client.get_json(
                url, {"iss.meta": "off", "iss.only": "timetable,dailytable"}
            )
            days = [
                (str(r["date"]), int(str(r["is_work_day"])), _opt(r.get("start_time")),
                 _opt(r.get("stop_time")))
                for r in _rows(data, "dailytable")
            ]  # fmt: skip
            week = [
                (int(str(r["week_day"])), int(str(r["is_work_day"])), _opt(r.get("start_time")),
                 _opt(r.get("stop_time")))
                for r in _rows(data, "timetable")
            ]  # fmt: skip
            if not days and not week:
                raise ValueError("пустой ответ")
            await asyncio.to_thread(repo.save_calendar, market, days, week, now)
        except Exception as exc:  # noqa: BLE001 - календарь не должен ронять обслуживание
            logger.warning("Ошибка загрузки календаря ISS %s: %s", market, exc)
            continue
        off = sum(1 for _, work, _, _ in days if not work)
        logger.info(
            "ISS_CALENDAR %s: дат %d (неторговых %d, торговых-исключений %d), с %s",
            market,
            len(days),
            off,
            len(days) - off,
            min((d for d, *_ in days), default="—"),
        )
        loaded[market] = len(days)
    return loaded


def is_trading_day(repo: IssTapeRepository, market: str, day: date) -> bool | None:
    """Торговый ли день рынка по календарю ISS; ``None`` — календарь не загружен.

    Исключение ``dailytable`` важнее недельного шаблона ``timetable``.
    """
    override = repo.calendar_day(market, day.isoformat())
    if override is not None:
        return bool(override)
    week = repo.week_timetable(market)
    if not week:
        return None
    work = week.get(day.isoweekday())
    return None if work is None else bool(work)


# ---------------------------------------------------------------------- #
# Торговые дни без данных (SPEC-0013 G-2)
# ---------------------------------------------------------------------- #
def _market(instrument: Instrument) -> str:
    return "shares" if instrument.type == InstrumentType.SHARE else "forts"


def _days(start: date, end: date) -> list[date]:
    return [start + timedelta(days=k) for k in range((end - start).days + 1)]


def _confirmed_no_trades(iss: IssTapeRepository, instrument: Instrument, day: date) -> bool:
    """Будний день с итогами ISS в кэше, где у инструмента (у фьючерса — у всех
    контрактов актива) 0 сделок: торгов действительно не было."""
    if day.weekday() >= 5:  # итоги выходных входят в понедельник
        return False
    totals = iss.cached_totals(_market(instrument), day.isoformat())
    if not totals:
        return False
    if instrument.type == InstrumentType.SHARE:
        trades = totals.get(instrument.id, (0, 0.0))[0]
    else:
        trades = sum(n for secid, (n, _v) in totals.items() if instrument.is_contract_ticker(secid))
    return trades == 0


def _contract_traded(
    iss: IssTapeRepository,
    instrument: Instrument,
    day: date,
    tape_cache: dict[date, dict[str, float]],
) -> bool:
    """SPEC-0014 G-5: у контракта за календарный день были сделки по данным ISS.

    Будний день с итогами — по итогам ``secid``; выходной или праздник (итогов
    своих нет) — по ленте ISS торгового дня, сделки которой пришлись на этот
    календарный день (``tape_cache`` — одна выборка на день для всех
    хранилищ). Нет ни итогов, ни ленты — доказательства нет: не пропуск.
    """
    totals = iss.cached_totals("forts", day.isoformat())
    if totals:
        return totals.get(instrument.id, (0, 0.0))[0] > 0
    if day not in tape_cache:
        start = datetime(day.year, day.month, day.day, tzinfo=UTC)
        trading = day
        for offset in range(1, 6):  # торговый день, к которому относится выходной
            trading = day + timedelta(days=offset)
            if iss.cached_totals("forts", trading.isoformat()):
                break
        tape_cache[day] = iss.tape_volumes(
            "forts", "RFUD", trading.isoformat(), since=start, before=start + timedelta(days=1)
        )
    return tape_cache[day].get(instrument.id, 0.0) > 0


def find_calendar_gaps(
    config: AppConfig,
    repos: Mapping[str, MarketDataRepository],
    iss: IssTapeRepository,
    today: date,
) -> dict[str, list[date]]:
    """Торговые по календарю ISS дни без данных — от первого сохранённого дня до вчера."""
    gaps: dict[str, list[date]] = {}
    tape_cache: dict[date, dict[str, float]] = {}
    for instrument in config.instruments:
        repo = repos[instrument.storage_name]
        have = repo.available_days()
        if instrument.is_contract_storage and instrument.window is not None:
            # SPEC-0014 G-5: только окно контракта; день без сделок по ISS — не пропуск;
            # дни первичной догрузки ещё не обработаны фоновым проходом.
            pending = set(repo.days_to_refresh())
            found = [
                day
                for day in _days(instrument.window[0], min(instrument.window[1], today))
                if day < today
                and day.isoformat() not in have
                and day.isoformat() not in pending
                and _contract_traded(iss, instrument, day, tape_cache)
            ]
            if found:
                gaps[instrument.storage_name] = found
            continue
        if not have:
            continue
        day = min(date.fromisoformat(d) for d in have)
        found = []
        while day < today:
            if (
                day.isoformat() not in have
                and is_trading_day(iss, _market(instrument), day)
                and not _confirmed_no_trades(iss, instrument, day)
            ):
                found.append(day)
            day += timedelta(days=1)
        if found:
            gaps[instrument.storage_name] = found
    return gaps


def mark_calendar_gaps(
    config: AppConfig,
    repos: Mapping[str, MarketDataRepository],
    iss: IssTapeRepository,
    today: date,
) -> int:
    """Отмечает пропуски ``calendar_gap`` и пишет ERROR (SPEC-0013 G-2); число дней."""
    total = 0
    for name, days in find_calendar_gaps(config, repos, iss, today).items():
        for day in days:
            logger.error(
                "TRADING_DAY_MISSING %s %s: торговый день по календарю ISS, данных нет —"
                " отмечен для перекачки",
                name,
                day,
            )
        repos[name].mark_days_for_refresh([d.isoformat() for d in days], REFRESH_CALENDAR_GAP)
        total += len(days)
    return total


# ---------------------------------------------------------------------- #
# Этап 2: расписание сессий и режимы торгов (SPEC-0012 G-3)
# ---------------------------------------------------------------------- #
WEEKEND = "weekend"
AUCTION = "auction"
SOURCE_TINVEST = "tinvest"
SOURCE_ISS = "iss"
#: Расписание загружается на столько дней вперёд (включая сегодня).
SCHEDULE_DAYS = 7
#: Дубли интервалов T-Invest без суффикса сессии — не используются.
_GENERIC = frozenset({"regular_trading_session", "opening_auction", "closing_auction"})
_SESSION_REGIMES = {
    "regular_trading_session_morning": "morning",
    "regular_trading_session_main": "main",
    "regular_trading_session_evening": "evening",
}
#: Сессии ISS ``session_schedule`` FORTS → режимы для сверки начала.
_ISS_SESSIONS = {"morning_session": "morning", "main_session": "main", "evening_session": "evening"}


def kind_to_regime(kind: str) -> tuple[bool, str | None]:
    """Тип интервала T-Invest → ``(хранить ли, режим)``; режим ``None`` — пауза."""
    if kind.startswith("dealer_") or kind in _GENERIC:
        return False, None
    if "auction" in kind:
        return True, AUCTION
    if kind in _SESSION_REGIMES:
        return True, _SESSION_REGIMES[kind]
    if kind.startswith("holiday_trading_session"):
        return True, WEEKEND
    return True, None  # клиринг и прочие паузы


def intervals_from_days(
    days: Iterable[Any],
) -> dict[str, list[tuple[str, str | None, datetime, datetime]]]:
    """Дни ``TradingSchedules`` (SDK) → ``{день: [(kind, regime, начало, конец)]}``."""
    out: dict[str, list[tuple[str, str | None, datetime, datetime]]] = {}
    for day in days:
        moment = getattr(day, "date")
        key = (moment.date() if isinstance(moment, datetime) else moment).isoformat()
        rows = out.setdefault(key, [])
        for item in getattr(day, "intervals", None) or []:
            kind = str(getattr(item, "type", ""))
            keep, regime = kind_to_regime(kind)
            if not keep:
                continue
            interval = item.interval
            rows.append((kind, regime, interval.start_ts, interval.end_ts))
    return out


async def load_tinvest_schedule(
    instruments_service: Any,
    repo: IssTapeRepository,
    exchanges: Mapping[str, str],
    today: date,
) -> dict[str, int]:
    """Расписание T-Invest на ``SCHEDULE_DAYS`` дней по расписанию каждого рынка.

    ``exchanges`` — ``{рынок: расписание}`` (поле ``exchange`` инструментов).
    Ошибка рынка логируется, прежнее расписание остаётся (SPEC-0012 G-2).
    """
    loaded: dict[str, int] = {}
    now = datetime.now(UTC)
    start = datetime.combine(today, time(0), tzinfo=UTC)
    for market, exchange in exchanges.items():
        try:
            response = await instruments_service.trading_schedules(
                exchange=exchange,
                from_=max(start, now),
                to=start + timedelta(days=SCHEDULE_DAYS),
            )
            days: dict[str, list[tuple[str, str | None, datetime, datetime]]] = {}
            for item in getattr(response, "exchanges", None) or []:
                days.update(intervals_from_days(getattr(item, "days", None) or []))
            for day, rows in days.items():
                await asyncio.to_thread(
                    repo.replace_sessions,
                    market,
                    day,
                    SOURCE_TINVEST,
                    exchange,
                    rows,
                    now.isoformat(),
                )
        except Exception as exc:  # noqa: BLE001 - расписание не должно ронять обслуживание
            logger.warning("Ошибка загрузки расписания %s (%s): %s", market, exchange, exc)
            continue
        loaded[market] = len(days)
        logger.info(
            "SCHEDULE %s %s: дней %d, с %s по %s",
            market,
            exchange,
            len(days),
            min(days, default="—"),
            max(days, default="—"),
        )
    return loaded


async def check_iss_session_schedule(
    client: IssClient, repo: IssTapeRepository, today: date
) -> list[str]:
    """Сверяет начала сессий FORTS сегодня: T-Invest против ISS ``session_schedule``.

    Сохраняет интервалы ISS (источник ``iss``); расхождение — WARNING
    ``SCHEDULE_MISMATCH``. Возвращает список расхождений.
    """
    data = await client.get_json(
        ENGINE_URLS["forts"], {"iss.meta": "off", "iss.only": "session_schedule"}
    )
    rows: list[tuple[str, str | None, datetime, datetime]] = []
    for row in _rows(data, "session_schedule"):
        kind = str(row.get("type"))
        if kind not in _ISS_SESSIONS or not row.get("time_from") or not row.get("time_till"):
            continue
        if str(row.get("trade_session_date")) != today.isoformat():
            continue
        moment_from = datetime.fromisoformat(str(row["time_from"])).replace(tzinfo=MSK)
        moment_till = datetime.fromisoformat(str(row["time_till"])).replace(tzinfo=MSK)
        rows.append((kind, _ISS_SESSIONS[kind], moment_from, moment_till))
    if not rows:
        return []
    await asyncio.to_thread(
        repo.replace_sessions, "forts", today.isoformat(), SOURCE_ISS, SOURCE_ISS, rows,
        datetime.now(UTC).isoformat(),
    )  # fmt: skip
    ours = {
        regime: start
        for _kind, regime, start, _end in await asyncio.to_thread(
            repo.sessions, "forts", today.isoformat(), SOURCE_TINVEST
        )
        if regime in ("morning", "main", "evening")
    }
    mismatches: list[str] = []
    for _kind, regime, start, _end in rows:
        mine = ours.get(str(regime))
        if mine is None or abs((mine - start).total_seconds()) >= 60:
            text = (
                f"сессия {regime} — T-Invest "
                f"{'нет' if mine is None else f'{mine.astimezone(MSK):%H:%M}'},"
                f" ISS {start.astimezone(MSK):%H:%M}"
            )
            logger.warning("SCHEDULE_MISMATCH forts %s: %s", today, text)
            mismatches.append(text)
    return mismatches


def _hhmm(value: str) -> int:
    hours, minutes = value.split(":")
    return int(hours) * 60 + int(minutes)


def regime_of(moment: datetime, market: str, config: SignalsConfig) -> str | None:
    """Режим момента по границам из конфигурации (МСК); ``None`` — вне режимов."""
    local = moment.astimezone(MSK)
    if local.weekday() >= 5:
        return WEEKEND
    minute = local.hour * 60 + local.minute
    for name, (start, end) in config.regimes[market].items():
        if _hhmm(start) <= minute < _hhmm(end):
            return name
    return None


def regimes_of(stamps: pd.Series, market: str, config: SignalsConfig) -> np.ndarray:
    """Режимы серии моментов по конфигурации (векторно); ``""`` — вне режимов."""
    local = pd.to_datetime(stamps, utc=True).dt.tz_convert(MSK)
    minute = (local.dt.hour * 60 + local.dt.minute).to_numpy()
    labels = np.full(len(local), "", dtype=object)
    for name, (start, end) in config.regimes[market].items():
        labels[(minute >= _hhmm(start)) & (minute < _hhmm(end))] = name
    labels[(local.dt.weekday >= 5).to_numpy()] = WEEKEND
    return labels


Interval = tuple[datetime, datetime, str]  # начало, конец (не включая), режим


class Regimes:
    """Режим торгов момента: по расписанию его дня (SPEC-0012 G-3), иначе по конфигурации.

    Конец интервала T-Invest включает его последнюю минуту (``08:59`` — до 09:00).
    Расписание дня кэшируется; :meth:`clear` — после ночной загрузки.
    """

    def __init__(self, config: SignalsConfig, repo: IssTapeRepository | None = None) -> None:
        self._config = config
        self._repo = repo
        self._cache: dict[tuple[str, date], list[Interval]] = {}

    def clear(self) -> None:
        self._cache.clear()

    def _day(self, market: str, day: date) -> list[Interval]:
        key = (market, day)
        if key not in self._cache:
            rows = [] if self._repo is None else self._repo.sessions(market, day.isoformat())
            self._cache[key] = [
                (start, end.replace(second=0, microsecond=0) + timedelta(minutes=1), regime)
                for _kind, regime, start, end in rows
                if regime is not None
            ]
        return self._cache[key]

    def regime(self, moment: datetime, market: str) -> str | None:
        intervals = self._day(market, moment.astimezone(MSK).date())
        if not intervals:
            return regime_of(moment, market, self._config)
        for start, end, regime in intervals:
            if start <= moment < end:
                return regime
        return None

    def labels(self, stamps: pd.Series, market: str) -> np.ndarray:
        """Режимы серии моментов (векторно); ``""`` — вне режимов."""
        times = pd.to_datetime(stamps, utc=True)
        labels = regimes_of(times, market, self._config)
        days = times.dt.tz_convert(MSK).dt.date.to_numpy()
        values = times.dt.tz_convert(UTC).dt.tz_localize(None).to_numpy()
        for day in set(days):
            intervals = self._day(market, day)
            if not intervals:
                continue
            mask = days == day
            day_labels = np.full(int(mask.sum()), "", dtype=object)
            day_values = values[mask]
            for start, end, regime in intervals:
                inside = (
                    day_values >= np.datetime64(start.astimezone(UTC).replace(tzinfo=None))
                ) & (day_values < np.datetime64(end.astimezone(UTC).replace(tzinfo=None)))
                day_labels[inside] = regime
            labels[mask] = day_labels
        return labels
