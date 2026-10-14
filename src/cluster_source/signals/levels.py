"""Кластерные сигналы MVP, этап 2 — класс ``level`` (SPEC-0008).

Бар (по умолчанию 5 минут) раскладывается по ценовым уровням (шаг цены
инструмента): агрессивные покупки, продажи и прочий объём (аукционы,
``UNKNOWN`` — в объём, но не в агрессию). На закрытии бара:

* ``stacked_imbalance`` — диагональный дисбаланс: покупки на P против продаж
  на P − шаг ≥ ``stacked_ratio`` (или продажи на P против покупок на P + шаг)
  на ≥ ``stacked_levels`` уровнях подряд; доминирующий объём уровня — не ниже
  перцентиля ``stacked_level_percentile`` объёмов уровня по нормам.
* ``absorption`` — уровень с наибольшим агрессивным объёмом одной стороны
  лежит на экстремуме бара по ходу агрессии (продажи — на минимуме, покупки —
  на максимуме), его объём ≥ перцентиля ``absorption_percentile`` по нормам,
  закрытие — не ближе ``absorption_close_ticks`` шагов к этому экстремуму.

Те же функции пересчитывают сигнал на исправленных данных (``REPAIRED``):
раскладка по уровням не зависит от точности времени сделок.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any

import pandas as pd

from cluster_source.collector import Tick
from cluster_source.config import Direction, Instrument, SignalsConfig
from cluster_source.quality import minute_floor
from cluster_source.signals.baseline import Norms, market_of, regime_of
from cluster_source.signals.detectors import MINUTE_GRACE, Candidate

STACKED_IMBALANCE = "stacked_imbalance"
ABSORPTION = "absorption"
CLASS_LEVEL = "level"

Finding = tuple[str, str, dict[str, Any]]  # (event_type, direction, payload)


def bar_start(moment: datetime, minutes: int) -> datetime:
    start = minute_floor(moment)
    return start - timedelta(minutes=start.minute % minutes)


@dataclass
class Bar:
    """Бар по ценовым уровням: ``levels[k] = [покупки, продажи, прочее]``, k = цена / шаг."""

    start: datetime
    tick_size: float
    levels: dict[int, list[float]] = field(default_factory=dict)
    open: float | None = None
    high: float | None = None
    low: float | None = None
    close: float | None = None
    volume: float = 0.0
    delta: float = 0.0

    def level(self, price: float) -> int:
        return round(price / self.tick_size)

    def add(self, price: float, volume: float, direction: str) -> None:
        if self.open is None:
            self.open = self.high = self.low = price
        assert self.high is not None and self.low is not None
        self.high, self.low, self.close = max(self.high, price), min(self.low, price), price
        cell = self.levels.setdefault(self.level(price), [0.0, 0.0, 0.0])
        if direction == Direction.BUY.value:
            cell[0] += volume
            self.delta += volume
        elif direction == Direction.SELL.value:
            cell[1] += volume
            self.delta -= volume
        else:
            cell[2] += volume
        self.volume += volume

    @classmethod
    def from_frame(cls, start: datetime, df: pd.DataFrame, tick_size: float) -> Bar:
        """Бар из сделок хранилища (по времени) — для пересчёта на исправленных данных."""
        bar = cls(start, tick_size)
        if not df.empty:
            ordered = df.sort_values("timestamp", kind="stable")
            for price, volume, direction in zip(
                ordered["price"], ordered["volume"], ordered["direction"], strict=True
            ):
                bar.add(float(price), float(volume), str(direction))
        return bar

    def summary(self) -> dict[str, float]:
        return {
            "open": float(self.open or 0.0),
            "high": float(self.high or 0.0),
            "low": float(self.low or 0.0),
            "close": float(self.close or 0.0),
            "volume": self.volume,
            "delta": self.delta,
        }

    def price(self, k: int) -> float:
        return round(k * self.tick_size, 10)


def _ratio(x: float, y: float) -> float:
    """Отношение объёмов; знаменатель не меньше 1 лота (нет встречных сделок)."""
    return round(x / max(y, 1.0), 3)


def stacked_imbalances(
    bar: Bar, ratio_min: float, levels_min: int, level_min: float
) -> list[Finding]:
    """Самая длинная серия диагональных дисбалансов каждой стороны (≥ ``levels_min``)."""
    found: list[Finding] = []
    for side, direction in ((Direction.BUY.value, "long"), (Direction.SELL.value, "short")):
        run: list[dict[str, float]] = []
        best: list[dict[str, float]] = []
        prev: int | None = None
        for k in sorted(bar.levels):
            cell = bar.levels[k]
            if side == Direction.BUY.value:  # покупки на P против продаж на P − шаг
                mine, other = cell[0], bar.levels.get(k - 1, [0.0, 0.0, 0.0])[1]
                row = {"buy_volume": mine, "sell_volume": other}
            else:  # продажи на P против покупок на P + шаг
                mine, other = cell[1], bar.levels.get(k + 1, [0.0, 0.0, 0.0])[0]
                row = {"buy_volume": other, "sell_volume": mine}
            hit = mine >= level_min and mine >= ratio_min * max(other, 1.0)
            if hit and prev is not None and k == prev + 1 and run:
                run.append({"price": bar.price(k), **row, "ratio": _ratio(mine, other)})
            elif hit:
                run = [{"price": bar.price(k), **row, "ratio": _ratio(mine, other)}]
            else:
                run = []
            prev = k
            if len(run) > len(best):
                best = list(run)
        if len(best) >= levels_min:
            found.append(
                (
                    STACKED_IMBALANCE,
                    direction,
                    {
                        "side": side,
                        "levels": best,
                        "ratio_min": ratio_min,
                        "level_min": level_min,
                        "bar": bar.summary(),
                    },
                )
            )
    return found


def absorptions(bar: Bar, threshold: float, close_ticks: int) -> list[Finding]:
    """Поглощение агрессии на экстремуме бара с отскоком закрытия."""
    if not bar.levels or bar.close is None:
        return []
    found: list[Finding] = []
    low_k, high_k, close_k = min(bar.levels), max(bar.levels), bar.level(bar.close)
    for side, idx, extreme, direction in (
        (Direction.SELL.value, 1, low_k, "long"),  # продажи поглощены на минимуме
        (Direction.BUY.value, 0, high_k, "short"),  # покупки поглощены на максимуме
    ):
        k = max(bar.levels, key=lambda lv: bar.levels[lv][idx])
        volume = bar.levels[k][idx]
        distance = abs(close_k - extreme)
        if k != extreme or volume <= 0 or volume < threshold or distance < close_ticks:
            continue
        found.append(
            (
                ABSORPTION,
                direction,
                {
                    "level_price": bar.price(k),
                    "absorbed_side": side,
                    "level_volume": volume,
                    "threshold": threshold,
                    "close_distance_ticks": int(distance),
                    "min_close_ticks": close_ticks,
                    "bar": bar.summary(),
                },
            )
        )
    return found


def evaluate_bar(bar: Bar, norms: Norms, config: SignalsConfig, market: str) -> list[Finding]:
    """Сигналы закрытого бара по нормам режима (нет нормы — детектор режима молчит)."""
    regime = regime_of(bar.start, market, config) or ""
    found: list[Finding] = []
    level_min = norms.level_volume_pct.get(regime)
    if level_min is not None:
        found += stacked_imbalances(bar, config.stacked_ratio, config.stacked_levels, level_min)
    threshold = norms.absorption_threshold.get(regime)
    if threshold is not None:
        found += absorptions(bar, threshold, config.absorption_close_ticks)
    for _type, _direction, payload in found:
        payload["regime"] = regime
    return found


def recheck_level(event_type: str, direction: str, payload: dict[str, Any], bar: Bar) -> bool:
    """Сигнал в силе на исправленных данных бара: те же пороги из ``payload``."""
    if event_type == STACKED_IMBALANCE:
        found = stacked_imbalances(
            bar,
            float(payload["ratio_min"]),
            len(payload["levels"]),
            float(payload["level_min"]),
        )
    else:
        found = absorptions(bar, float(payload["threshold"]), int(payload["min_close_ticks"]))
    return any(t == event_type and d == direction for t, d, _ in found)


class LevelDetector:
    """Кластерные сигналы на закрытии бара ``level_bar_minutes``."""

    def __init__(self, instrument: Instrument, norms: Norms, config: SignalsConfig) -> None:
        self._instrument = instrument
        self._norms = norms
        self._config = config
        self._market = market_of(instrument)
        self._minutes = config.level_bar_minutes
        self.reset()

    def reset(self) -> None:
        self._bar: Bar | None = None

    def on_tick(self, tick: Tick, now: datetime) -> list[Candidate]:
        start = bar_start(tick.timestamp, self._minutes)
        out: list[Candidate] = []
        if self._bar is not None and start > self._bar.start:
            out = self._close()
        if self._bar is None:
            self._bar = Bar(start, self._instrument.tick_size)
        if start == self._bar.start:
            self._bar.add(tick.price, float(tick.volume), tick.direction)
        return out

    def flush(self, now: datetime) -> list[Candidate]:
        if self._bar is not None and now >= self._end(self._bar) + MINUTE_GRACE:
            return self._close()
        return []

    def _end(self, bar: Bar) -> datetime:
        return bar.start + timedelta(minutes=self._minutes)

    def _close(self) -> list[Candidate]:
        bar, self._bar = self._bar, None
        if bar is None:
            return []
        end = self._end(bar)
        return [
            Candidate(
                event_type=event_type,
                cls=CLASS_LEVEL,
                direction=direction,
                timeframe=f"{self._minutes}m",
                window_from=bar.start,
                window_to=end,
                bar_closed=True,
                moment=end,
                key_extra="",
                payload=payload,
            )
            for event_type, direction, payload in evaluate_bar(
                bar, self._norms, self._config, self._market
            )
        ]
