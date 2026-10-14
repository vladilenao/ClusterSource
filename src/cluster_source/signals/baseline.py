"""Нормы детекторов по окну окончательных дней (SPEC-0008, SPEC-0001 G-3).

Окно — ``reader.get_baseline_days``: ``FINAL`` первыми, добор
``ARCHIVE_CONSISTENT`` (SPEC-0006). Нормы считаются отдельно для режима торгов
(утро / основная / вечер / выходные): уровни активности сессий различаются в
разы. Торговый день понедельника включает сессии субботы и воскресенья.
"""

from __future__ import annotations

import logging
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta

import numpy as np
import pandas as pd

from cluster_source.config import Direction, Instrument, InstrumentType, SignalsConfig
from cluster_source.database import MarketDataRepository
from cluster_source.iss import MSK
from cluster_source.reader import get_baseline_days

logger = logging.getLogger(__name__)

WEEKEND = "weekend"
#: Меньше стольких родительских заявок режима — порог не считается (детектор режима выключен).
MIN_PARENT_ORDERS = 200
#: Меньше стольких минут со сделками в режиме — перцентиль минутного объёма не считается.
MIN_MINUTES = 30
#: Меньше стольких уровней баров в режиме — пороги кластерных сигналов не считаются.
MIN_LEVELS = 200


def market_of(instrument: Instrument) -> str:
    return "shares" if instrument.type == InstrumentType.SHARE else "forts"


def _hhmm(value: str) -> int:
    hours, minutes = value.split(":")
    return int(hours) * 60 + int(minutes)


def regime_of(moment: datetime, market: str, config: SignalsConfig) -> str | None:
    """Режим торгов момента (МСК); ``None`` — вне всех режимов (пауза, клиринг)."""
    local = moment.astimezone(MSK)
    if local.weekday() >= 5:
        return WEEKEND
    minute = local.hour * 60 + local.minute
    for name, (start, end) in config.regimes[market].items():
        if _hhmm(start) <= minute < _hhmm(end):
            return name
    return None


def regimes_of(stamps: pd.Series, market: str, config: SignalsConfig) -> np.ndarray:
    """Режимы торгов для серии моментов (векторно); ``""`` — вне режимов."""
    local = pd.to_datetime(stamps, utc=True).dt.tz_convert(MSK)
    minute = (local.dt.hour * 60 + local.dt.minute).to_numpy()
    labels = np.full(len(local), "", dtype=object)
    for name, (start, end) in config.regimes[market].items():
        labels[(minute >= _hhmm(start)) & (minute < _hhmm(end))] = name
    labels[(local.dt.weekday >= 5).to_numpy()] = WEEKEND
    return labels


def minute_slot(moment: datetime) -> str:
    """Минута дня МСК с видом дня: ``weekday 10:31`` / ``weekend 12:00``."""
    local = moment.astimezone(MSK)
    kind = WEEKEND if local.weekday() >= 5 else "weekday"
    return f"{kind} {local:%H:%M}"


def parent_sizes(df: pd.DataFrame, instrument: Instrument) -> pd.DataFrame:
    """Родительские заявки: сделки с одинаковым временем и стороной (без ``UNKNOWN``).

    Размер — в рублях у акций (``Σ price × volume × lot_size``), в контрактах у
    фьючерсов. Колонки: ``timestamp``, ``direction``, ``volume``, ``size``,
    ``trades``, ``price_min``, ``price_max``.
    """
    known = df.loc[df["direction"] != Direction.UNKNOWN.value]
    if known.empty:
        return pd.DataFrame(
            columns=["timestamp", "direction", "volume", "size", "trades", "price_min", "price_max"]
        )
    known = known.assign(notional=known["price"] * known["volume"])
    grouped = known.groupby(["timestamp", "direction"], sort=True).agg(
        volume=("volume", "sum"),
        notional=("notional", "sum"),
        trades=("volume", "size"),
        price_min=("price", "min"),
        price_max=("price", "max"),
    )
    grouped = grouped.reset_index()
    if instrument.type == InstrumentType.SHARE:
        grouped["size"] = grouped["notional"] * instrument.lot_size
    else:
        grouped["size"] = grouped["volume"].astype(float)
    return grouped.drop(columns=["notional"])


@dataclass(frozen=True)
class Norms:
    """Нормы инструмента по окну ``baseline_days``."""

    days_final: int
    days_archive: int
    #: Порог размера родительской заявки по режиму (перцентиль ``large_aggressor_percentile``).
    aggressor_threshold: dict[str, float] = field(default_factory=dict)
    #: Медиана объёма минуты дня по слоту ``minute_slot`` (дни без сделок — нули).
    minute_median: dict[str, float] = field(default_factory=dict)
    #: Перцентиль ``volume_spike_percentile`` минутных объёмов по режиму.
    minute_volume_pct: dict[str, float] = field(default_factory=dict)
    #: Этап 2: перцентиль ``stacked_level_percentile`` объёмов уровня бара по режиму.
    level_volume_pct: dict[str, float] = field(default_factory=dict)
    #: Этап 2: перцентиль ``absorption_percentile`` агрессивного объёма стороны на уровне.
    absorption_threshold: dict[str, float] = field(default_factory=dict)

    @property
    def days(self) -> int:
        return self.days_final + self.days_archive

    @property
    def quality(self) -> str:
        """``pure`` — только ``FINAL``; ``mixed`` — есть ``ARCHIVE_CONSISTENT`` (SPEC-0006)."""
        return "pure" if self.days_archive == 0 else "mixed"

    @property
    def baseline_days(self) -> dict[str, int]:
        return {"final": self.days_final, "archive": self.days_archive}


def _calendar_days(trade_date: date) -> list[date]:
    """Календарные дни торгового дня: понедельник включает субботу и воскресенье."""
    days = [trade_date]
    for back in (1, 2):
        prev = trade_date - timedelta(days=back)
        if prev.weekday() >= 5:
            days.append(prev)
    return days


def compute_norms(
    repo: MarketDataRepository, instrument: Instrument, config: SignalsConfig
) -> Norms | None:
    """Нормы по окну ``get_baseline_days`` (блокирующий вызов — через ``to_thread``).

    ``None`` — дней в окне меньше ``baseline_min_days``: детекторы инструмента
    выключены (SPEC-0008 G-6).
    """
    window = get_baseline_days(repo, n=config.baseline_days)
    if len(window) < config.baseline_min_days:
        return None
    market = market_of(instrument)
    sizes: dict[str, list[np.ndarray]] = defaultdict(list)
    minute_volumes: dict[str, list[np.ndarray]] = defaultdict(list)
    per_slot: dict[str, list[float]] = defaultdict(list)
    level_totals: dict[str, list[np.ndarray]] = defaultdict(list)
    level_sides: dict[str, list[np.ndarray]] = defaultdict(list)
    day_kinds: dict[str, int] = defaultdict(int)
    for trade_date, _status in window:
        for day in _calendar_days(trade_date):
            df = repo.read_day(day.isoformat())
            if df.empty:
                continue
            kind = WEEKEND if day.weekday() >= 5 else "weekday"
            day_kinds[kind] += 1
            parents = parent_sizes(df, instrument)
            labels = regimes_of(parents["timestamp"], market, config)
            values = parents["size"].to_numpy(dtype=float)
            for regime in set(labels) - {""}:
                sizes[str(regime)].append(values[labels == regime])
            minutes = df.groupby(df["timestamp"].dt.floor("min"))["volume"].sum()
            stamps = pd.Series(minutes.index)
            minute_labels = regimes_of(stamps, market, config)
            volumes = minutes.to_numpy(dtype=float)
            for regime in set(minute_labels) - {""}:
                minute_volumes[str(regime)].append(volumes[minute_labels == regime])
            local = stamps.dt.tz_convert(MSK)
            slots = np.where(local.dt.weekday >= 5, "weekend ", "weekday ") + local.dt.strftime(
                "%H:%M"
            ).to_numpy(dtype=str)
            for slot, volume in zip(slots, volumes, strict=True):
                per_slot[str(slot)].append(float(volume))
            totals, sides = _level_volumes(df, instrument, config, market)
            for regime, values in totals.items():
                level_totals[regime].append(values)
            for regime, values in sides.items():
                level_sides[regime].append(values)
    thresholds = {
        regime: float(np.percentile(np.concatenate(parts), config.large_aggressor_percentile))
        for regime, parts in sizes.items()
        if sum(len(p) for p in parts) >= MIN_PARENT_ORDERS
    }
    pct = {
        regime: float(np.percentile(np.concatenate(parts), config.volume_spike_percentile))
        for regime, parts in minute_volumes.items()
        if sum(len(p) for p in parts) >= MIN_MINUTES
    }
    medians: dict[str, float] = {}
    for slot, slot_volumes in per_slot.items():
        days_of_kind = day_kinds[slot.split(" ", 1)[0]]
        padded = slot_volumes + [0.0] * max(0, days_of_kind - len(slot_volumes))  # без сделок
        medians[slot] = float(np.median(padded))
    level_pct = _percentiles(level_totals, config.stacked_level_percentile, MIN_LEVELS)
    absorption = _percentiles(level_sides, config.absorption_percentile, MIN_LEVELS)
    final = sum(1 for _, status in window if status == "FINAL")
    return Norms(final, len(window) - final, thresholds, medians, pct, level_pct, absorption)


def _percentiles(
    parts: dict[str, list[np.ndarray]], percentile: float, minimum: int
) -> dict[str, float]:
    return {
        regime: float(np.percentile(np.concatenate(chunks), percentile))
        for regime, chunks in parts.items()
        if sum(len(c) for c in chunks) >= minimum
    }


def _level_volumes(
    df: pd.DataFrame, instrument: Instrument, config: SignalsConfig, market: str
) -> tuple[dict[str, np.ndarray], dict[str, np.ndarray]]:
    """Объёмы уровней баров ``level_bar_minutes`` по режиму: всего на уровне и
    агрессивный объём каждой стороны (ненулевой) — для этапа 2 SPEC-0008."""
    bars = df["timestamp"].dt.floor(f"{config.level_bar_minutes}min")
    level = (df["price"] / instrument.tick_size).round().astype("int64")
    buy = df["volume"].where(df["direction"] == Direction.BUY.value, 0.0)
    sell = df["volume"].where(df["direction"] == Direction.SELL.value, 0.0)
    frame = pd.DataFrame({"bar": bars, "level": level, "total": df["volume"], "buy": buy,
                          "sell": sell})  # fmt: skip
    cells = frame.groupby(["bar", "level"], sort=False).sum().reset_index()
    labels = regimes_of(cells["bar"], market, config)
    totals: dict[str, np.ndarray] = {}
    sides: dict[str, np.ndarray] = {}
    for regime in set(labels) - {""}:
        part = cells.loc[labels == regime]
        totals[str(regime)] = part["total"].to_numpy(dtype=float)
        both = np.concatenate(
            [part["buy"].to_numpy(dtype=float), part["sell"].to_numpy(dtype=float)]
        )
        sides[str(regime)] = both[both > 0]
    return totals, sides
