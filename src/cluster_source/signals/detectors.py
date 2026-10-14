"""Детекторы сигналов MVP, этап 1 — класс ``fast`` (SPEC-0008).

Детекторы получают тики из разветвления потока коллектора (G-2) и работают
только в памяти: результат — кандидат в сигнал, который движок проверяет по
G-3 (качество данных) и публикует.

* ``large_aggressor`` — родительская заявка: сделки с одинаковым временем (до
  микросекунды) и стороной; размер ≥ перцентиля по нормам режима.
* ``volume_spike`` — на закрытии минуты: объём / медиана объёма той же минуты
  дня ≥ ``k`` и объём ≥ перцентиля минутных объёмов режима.

Пересчёт на исправленных данных (``REPAIRED``, SPEC-0005) — по параметрам из
``payload`` сигнала, без текущих норм.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any

import pandas as pd

from cluster_source.collector import Tick
from cluster_source.config import Direction, Instrument, InstrumentType, SignalsConfig
from cluster_source.quality import minute_floor
from cluster_source.signals.baseline import Norms, market_of, minute_slot, regime_of
from cluster_source.signals.events import iso

LARGE_AGGRESSOR = "large_aggressor"
VOLUME_SPIKE = "volume_spike"
CLASS_FAST = "fast"

#: Группа заявки закрывается, если новых сделок с её временем нет столько секунд.
GROUP_IDLE = timedelta(seconds=2)
#: Минута закрывается через столько секунд после конца (запаздывание стрима).
MINUTE_GRACE = timedelta(seconds=2)
#: Дельта меньше этой доли объёма — направление ``neutral``.
NEUTRAL_DELTA_SHARE = 0.2

_SIDE_TO_DIRECTION = {Direction.BUY.value: "long", Direction.SELL.value: "short"}


@dataclass(frozen=True)
class Candidate:
    """Кандидат в сигнал — до проверки качества данных и публикации."""

    event_type: str
    cls: str
    direction: str
    timeframe: str
    window_from: datetime
    window_to: datetime
    bar_closed: bool
    #: Время рынка события: момент заявки / конец закрытой минуты.
    moment: datetime
    #: Различает сигналы одного окна (время заявки) — входит в ``event_id``.
    key_extra: str
    payload: dict[str, Any] = field(default_factory=dict)


def _size(price_volume: float, volume: float, instrument: Instrument) -> float:
    """Размер заявки: рубли у акций, контракты у фьючерсов."""
    if instrument.type == InstrumentType.SHARE:
        return price_volume * instrument.lot_size
    return volume


class LargeAggressorDetector:
    """Крупная агрессивная заявка (родительская заявка по времени и стороне)."""

    def __init__(self, instrument: Instrument, norms: Norms, config: SignalsConfig) -> None:
        self._instrument = instrument
        self._norms = norms
        self._config = config
        self._market = market_of(instrument)
        self.reset()

    def reset(self) -> None:
        self._key: tuple[datetime, str] | None = None
        self._volume = 0
        self._notional = 0.0
        self._trades = 0
        self._pmin = 0.0
        self._pmax = 0.0
        self._seen_at: datetime | None = None

    def on_tick(self, tick: Tick, now: datetime) -> list[Candidate]:
        if tick.direction not in _SIDE_TO_DIRECTION:  # аукционы — не агрессия (SPEC-0008)
            return []
        key = (tick.timestamp, tick.direction)
        out: list[Candidate] = []
        if key != self._key:
            out = self._close()
            self._key = key
            self._volume, self._notional, self._trades = 0, 0.0, 0
            self._pmin = self._pmax = tick.price
        self._volume += tick.volume
        self._notional += tick.price * tick.volume
        self._trades += 1
        self._pmin, self._pmax = min(self._pmin, tick.price), max(self._pmax, tick.price)
        self._seen_at = now
        return out

    def flush(self, now: datetime) -> list[Candidate]:
        if (
            self._key is not None
            and self._seen_at is not None
            and now - self._seen_at >= GROUP_IDLE
        ):
            return self._close()
        return []

    def _close(self) -> list[Candidate]:
        if self._key is None:
            return []
        moment, side = self._key
        self._key = None
        regime = regime_of(moment, self._market, self._config)
        threshold = self._norms.aggressor_threshold.get(regime or "")
        size = _size(self._notional, self._volume, self._instrument)
        if threshold is None or size < threshold:
            return []
        start = minute_floor(moment)
        return [
            Candidate(
                event_type=LARGE_AGGRESSOR,
                cls=CLASS_FAST,
                direction=_SIDE_TO_DIRECTION[side],
                timeframe="tick",
                window_from=start,
                window_to=start + timedelta(minutes=1),
                bar_closed=False,
                moment=moment,
                key_extra=iso(moment),
                payload={
                    "trade_time": iso(moment),
                    "side": side,
                    "price_min": self._pmin,
                    "price_max": self._pmax,
                    "quantity": int(self._volume),
                    "trades": self._trades,
                    "size": round(size, 2),
                    "size_unit": "rub"
                    if self._instrument.type == InstrumentType.SHARE
                    else "contracts",
                    "threshold": round(threshold, 2),
                    "percentile": self._config.large_aggressor_percentile,
                    "regime": regime,
                },
            )
        ]


def recheck_large_aggressor(
    payload: dict[str, Any], second: pd.DataFrame, instrument: Instrument
) -> bool:
    """Сигнал в силе после дозаливки (SPEC-0008).

    ``second`` — сделки секунды заявки из хранилища (с колонкой ``origin``).
    Секунда не заменялась сделками ISS — заявка та же; заменялась — заявка
    пересчитывается по сделкам ISS этой стороны (у них время до секунды).
    """
    if second.empty or not (second["origin"] == "iss").any():
        return True
    side = second.loc[(second["origin"] == "iss") & (second["direction"] == payload["side"])]
    if side.empty:
        return False
    notional = float((side["price"] * side["volume"]).sum())
    return _size(notional, float(side["volume"].sum()), instrument) >= float(payload["threshold"])


class VolumeSpikeDetector:
    """Всплеск объёма минуты относительно той же минуты дня (RVOL)."""

    def __init__(self, instrument: Instrument, norms: Norms, config: SignalsConfig) -> None:
        self._instrument = instrument
        self._norms = norms
        self._config = config
        self._market = market_of(instrument)
        self.reset()

    def reset(self) -> None:
        self._minute: datetime | None = None
        self._volume = 0.0
        self._buy = 0.0
        self._sell = 0.0

    def on_tick(self, tick: Tick, now: datetime) -> list[Candidate]:
        minute = minute_floor(tick.timestamp)
        out: list[Candidate] = []
        if self._minute is not None and minute > self._minute:
            out = self._close()
        if self._minute is None:
            self._minute = minute
            self._volume = self._buy = self._sell = 0.0
        if minute == self._minute:
            self._volume += tick.volume
            if tick.direction == Direction.BUY.value:
                self._buy += tick.volume
            elif tick.direction == Direction.SELL.value:
                self._sell += tick.volume
        return out

    def flush(self, now: datetime) -> list[Candidate]:
        if self._minute is not None and now >= self._minute + timedelta(minutes=1) + MINUTE_GRACE:
            return self._close()
        return []

    def _close(self) -> list[Candidate]:
        if self._minute is None:
            return []
        minute, volume, delta = self._minute, self._volume, self._buy - self._sell
        self._minute = None
        spike = evaluate_spike(minute, volume, delta, self._norms, self._config, self._market)
        if spike is None:
            return []
        direction, payload = spike
        end = minute + timedelta(minutes=1)
        return [
            Candidate(
                event_type=VOLUME_SPIKE,
                cls=CLASS_FAST,
                direction=direction,
                timeframe="1m",
                window_from=minute,
                window_to=end,
                bar_closed=True,
                moment=end,
                key_extra="",
                payload=payload,
            )
        ]


def spike_direction(volume: float, delta: float) -> str:
    if volume <= 0 or abs(delta) < NEUTRAL_DELTA_SHARE * volume:
        return "neutral"
    return "long" if delta > 0 else "short"


def evaluate_spike(
    minute: datetime,
    volume: float,
    delta: float,
    norms: Norms,
    config: SignalsConfig,
    market: str,
) -> tuple[str, dict[str, Any]] | None:
    """Правило ``volume_spike``: ``(направление, payload)`` или ``None``."""
    regime = regime_of(minute, market, config)
    pct = norms.minute_volume_pct.get(regime or "")
    median = norms.minute_median.get(minute_slot(minute))
    if pct is None or median is None or volume < pct:
        return None
    ratio = volume / max(median, 1.0)
    if ratio < config.volume_spike_ratio:
        return None
    return spike_direction(volume, delta), {
        "minute": iso(minute),
        "volume": volume,
        "baseline_median": median,
        "ratio": round(ratio, 3),
        "threshold_ratio": config.volume_spike_ratio,
        "volume_p90": pct,
        "delta": delta,
        "regime": regime,
    }


def recheck_volume_spike(payload: dict[str, Any], minute: pd.DataFrame, direction: str) -> bool:
    """Всплеск в силе на исправленных данных минуты: те же пороги, то же направление."""
    volume = float(minute["volume"].sum()) if not minute.empty else 0.0
    buys = minute.loc[minute["direction"] == Direction.BUY.value, "volume"].sum()
    sells = minute.loc[minute["direction"] == Direction.SELL.value, "volume"].sum()
    delta = float(buys - sells)
    if volume < float(payload["volume_p90"]):
        return False
    if volume / max(float(payload["baseline_median"]), 1.0) < float(payload["threshold_ratio"]):
        return False
    return spike_direction(volume, delta) == direction
