"""Движок сигналов: разветвление потока, отсев, выдача и проверка (SPEC-0008).

* **Разветвление (G-2).** Коллектор после фильтров отдаёт тик в
  :meth:`SignalEngine.feed` — неблокирующая очередь. Переполнение не
  задерживает сбор: сигналы инструмента приостанавливаются на минуты
  потерянных тиков (``signals_paused``).
* **Отсев (G-3, SPEC-0001 G-1).** Сигнал не выдаётся, если инструмент сейчас
  ``DEGRADED``, в окне есть минута с отметкой деградации или сигналы
  приостановлены. Отсеянный сигнал записывается со статусом ``SUPPRESSED`` и
  не публикуется.
* **Проверка (G-4, SPEC-0001 G-2).** Раз в 30 с непроверенные сигналы сверяются
  с вердиктами минут (SPEC-0005); к сроку ``window_to + 20 мин`` каждый получает
  ровно один итог.

Сигнал и итог пишутся в outbox одной транзакцией с записью сигнала; в Redis
их отправляет :class:`~cluster_source.signals.publisher.EventPublisher`.
"""

from __future__ import annotations

import asyncio
import json
import logging
from collections import Counter
from collections.abc import Callable, Mapping
from datetime import UTC, datetime, timedelta
from typing import Any, Protocol

import pandas as pd

from cluster_source.collector import DataCollector, Tick
from cluster_source.config import AppConfig, Instrument
from cluster_source.database import MarketDataRepository, SignalRepository
from cluster_source.quality import STATE_DEGRADED, minute_floor
from cluster_source.reconcile import STATUS_GAP, STATUS_REPAIRED
from cluster_source.signals import events as ev
from cluster_source.signals.baseline import Norms, compute_norms
from cluster_source.signals.detectors import (
    LARGE_AGGRESSOR,
    VOLUME_SPIKE,
    Candidate,
    LargeAggressorDetector,
    VolumeSpikeDetector,
    recheck_large_aggressor,
    recheck_volume_spike,
)

logger = logging.getLogger(__name__)

FANOUT_CAPACITY = 100_000
#: Приостановка сигналов после потери тика: до конца следующей минуты.
PAUSE_AFTER_DROP = timedelta(minutes=2)
VERIFY_INTERVAL_SEC = 30.0
FLUSH_INTERVAL_SEC = 1.0

STATUS_CONFIRMED = "CONFIRMED"
STATUS_REVOKED = "REVOKED"
SUPPRESS_DEGRADED = "degraded"
SUPPRESS_DEGRADED_MINUTE = "degraded_minute"
SUPPRESS_PAUSED = "signals_paused"


class Detector(Protocol):
    def reset(self) -> None: ...
    def on_tick(self, tick: Tick, now: datetime) -> list[Candidate]: ...
    def flush(self, now: datetime) -> list[Candidate]: ...


class SignalEngine:
    """Сигналы всех инструментов процесса."""

    def __init__(
        self,
        config: AppConfig,
        repos: Mapping[str, MarketDataRepository],
        collectors: Mapping[str, DataCollector],
        signal_repo: SignalRepository,
        secid_of: Callable[[str], str | None],
        *,
        now: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        self._config = config
        self._signals = config.signals
        self._repos = repos
        self._collectors = collectors
        self._repo = signal_repo
        self._secid_of = secid_of
        self._now = now
        self._instruments: dict[str, Instrument] = {
            i.storage_name: i for i in config.instruments if i.storage_name in collectors
        }
        self._queue: asyncio.Queue[tuple[str, Tick]] = asyncio.Queue(maxsize=FANOUT_CAPACITY)
        self._norms: dict[str, Norms] = {}
        self._detectors: dict[str, list[Detector]] = {}
        self._paused: dict[str, tuple[datetime, datetime]] = {}
        self.suppressed: Counter[str] = Counter()

    # ------------------------------------------------------------------ #
    # Разветвление потока (G-2)
    # ------------------------------------------------------------------ #
    def feed(self, name: str, tick: Tick) -> None:
        """Тик от коллектора; никогда не ждёт (SPEC-0008 G-2)."""
        try:
            self._queue.put_nowait((name, tick))
        except asyncio.QueueFull:
            self._pause(name, tick.timestamp)

    def _pause(self, name: str, moment: datetime) -> None:
        start = minute_floor(moment)
        until = start + PAUSE_AFTER_DROP
        previous = self._paused.get(name)
        if previous is None or previous[1] < start:
            logger.warning("SIGNALS_PAUSED %s: очередь детекторов переполнена", name)
            self._paused[name] = (start, until)
        else:
            self._paused[name] = (previous[0], max(previous[1], until))
        for detector in self._detectors.get(name, []):
            detector.reset()

    # ------------------------------------------------------------------ #
    # Нормы (G-6)
    # ------------------------------------------------------------------ #
    async def refresh_norms(self) -> None:
        """Пересчитывает нормы и детекторы (при старте и после ночной проверки FINAL)."""
        for name, instrument in self._instruments.items():
            try:
                norms = await asyncio.to_thread(
                    compute_norms, self._repos[name], instrument, self._signals
                )
            except Exception as exc:  # noqa: BLE001 - один инструмент не роняет движок
                logger.error("Ошибка расчёта норм %s: %s", name, exc)
                continue
            if norms is None:
                if name in self._norms or name not in self._detectors:
                    logger.warning(
                        "SIGNAL_DETECTOR_OFF %s: в окне норм меньше %d дней",
                        name,
                        self._signals.baseline_min_days,
                    )
                self._norms.pop(name, None)
                self._detectors[name] = []
                continue
            self._norms[name] = norms
            self._detectors[name] = [
                LargeAggressorDetector(instrument, norms, self._signals),
                VolumeSpikeDetector(instrument, norms, self._signals),
            ]
        logger.info(
            "Нормы сигналов: детекторы включены у %d из %d инструментов",
            len(self._norms),
            len(self._instruments),
        )

    # ------------------------------------------------------------------ #
    # Детектирование и выдача
    # ------------------------------------------------------------------ #
    async def run(self) -> None:
        loop = asyncio.get_running_loop()
        next_flush = loop.time() + FLUSH_INTERVAL_SEC
        while True:
            try:
                name, tick = await asyncio.wait_for(
                    self._queue.get(), timeout=max(0.0, next_flush - loop.time())
                )
                await self.process(name, tick)
            except TimeoutError:
                pass
            if loop.time() >= next_flush:
                await self.flush()
                next_flush = loop.time() + FLUSH_INTERVAL_SEC

    async def process(self, name: str, tick: Tick) -> None:
        now = self._now()
        for detector in self._detectors.get(name, []):
            for candidate in detector.on_tick(tick, now):
                await self.emit(name, candidate)

    async def flush(self) -> None:
        now = self._now()
        for name, detectors in self._detectors.items():
            for detector in detectors:
                for candidate in detector.flush(now):
                    await self.emit(name, candidate)

    def gate(self, name: str, candidate: Candidate) -> str | None:
        """Причина отсева сигнала или ``None`` (SPEC-0008 G-3, SPEC-0001 G-1)."""
        quality = self._collectors[name].quality
        if quality.state == STATE_DEGRADED:
            return SUPPRESS_DEGRADED
        if quality.marked_in(candidate.window_from, candidate.window_to):
            return SUPPRESS_DEGRADED_MINUTE
        paused = self._paused.get(name)
        if (
            paused is not None
            and candidate.window_from < paused[1]
            and paused[0] < candidate.window_to
        ):
            return SUPPRESS_PAUSED
        return None

    def build_signal(self, name: str, candidate: Candidate) -> ev.Event:
        norms = self._norms[name]
        instrument = self._instruments[name]
        secid = self._secid_of(name) or instrument.id
        key = f"{ev.iso(candidate.window_from)}/{ev.iso(candidate.window_to)}/{candidate.direction}"
        if candidate.key_extra:
            key += f"/{candidate.key_extra}"
        event = ev.envelope(
            ev.CATEGORY_SIGNAL, candidate.event_type, name, secid, candidate.moment, key
        )
        deadline = candidate.window_to + timedelta(minutes=self._signals.verify_deadline_min)
        event.update(
            {
                "signal_id": event["event_id"],
                "class": candidate.cls,
                "direction": candidate.direction,
                "timeframe": candidate.timeframe,
                "window_from": ev.iso(candidate.window_from),
                "window_to": ev.iso(candidate.window_to),
                "bar_closed": candidate.bar_closed,
                "quality": "PROVISIONAL",
                "baseline_quality": norms.quality,
                "baseline_days": norms.baseline_days,
                "verify_deadline": ev.iso(deadline),
                "payload": candidate.payload,
            }
        )
        return event

    async def emit(self, name: str, candidate: Candidate) -> None:
        if name not in self._norms:
            return
        event = self.build_signal(name, candidate)
        row = {
            "signal_id": event["signal_id"],
            "ticker": name,
            "event_type": candidate.event_type,
            "class": candidate.cls,
            "direction": candidate.direction,
            "window_from": event["window_from"],
            "window_to": event["window_to"],
            "verify_deadline": event["verify_deadline"],
        }
        now = ev.iso(self._now())
        reason = self.gate(name, candidate)
        if reason is not None:
            self.suppressed[reason] += 1
            logger.info("SIGNAL_SUPPRESSED %s %s: %s", name, candidate.event_type, reason)
            await asyncio.to_thread(self._repo.add_suppressed, row, reason, ev.dumps(event), now)
            return
        await asyncio.to_thread(self._repo.add_signal, row, ev.dumps(event), now)
        logger.info(
            "SIGNAL %s %s %s: окно %s–%s, %s",
            name,
            candidate.event_type,
            candidate.direction,
            event["window_from"],
            event["window_to"],
            json.dumps(candidate.payload, ensure_ascii=False),
        )

    # ------------------------------------------------------------------ #
    # Проверка (G-4)
    # ------------------------------------------------------------------ #
    async def verify_loop(self) -> None:
        while True:
            try:
                await self.verify_once()
            except Exception as exc:  # noqa: BLE001 - проверка не должна останавливаться
                logger.error("Ошибка проверки сигналов: %s", exc)
            await asyncio.sleep(VERIFY_INTERVAL_SEC)

    async def verify_once(self) -> None:
        now = self._now()
        for row in await asyncio.to_thread(self._repo.pending_signals):
            try:
                await self._verify(row, now)
            except Exception as exc:  # noqa: BLE001 - один сигнал не роняет проверку
                logger.error("Ошибка проверки сигнала %s: %s", row["signal_id"], exc)

    async def _verify(self, row: dict[str, object], now: datetime) -> None:
        name = str(row["ticker"])
        repo = self._repos[name]
        start = ev.parse_iso(str(row["window_from"]))
        end = ev.parse_iso(str(row["window_to"]))
        deadline = ev.parse_iso(str(row["verify_deadline"]))
        verdicts = await asyncio.to_thread(repo.read_minute_verdicts, start, end)
        signal = json.loads(str(row["event_json"]))
        gaps = verdicts.loc[verdicts["status"] == STATUS_GAP]
        if not gaps.empty:
            extra = gaps["detail"].astype(str).str.startswith("extra_trades").any()
            reason = ev.REASON_EXTRA_TRADES if extra else ev.REASON_GAP
            await self._resolve(signal, now, None, reason)
            return
        stored = await asyncio.to_thread(repo.verified_until)
        if stored is not None and datetime.fromisoformat(stored) >= end:
            if (verdicts["status"] == STATUS_REPAIRED).any():
                kept = await asyncio.to_thread(self._recheck, name, signal)
                if kept:
                    await self._resolve(signal, now, ev.OUTCOME_REPAIRED, None)
                else:
                    await self._resolve(signal, now, None, ev.REASON_REPAIRED_CHANGED)
            else:
                await self._resolve(signal, now, ev.OUTCOME_VERIFIED, None)
            return
        if now >= deadline:
            await self._resolve(signal, now, None, ev.REASON_UNAVAILABLE)

    def _recheck(self, name: str, signal: dict[str, Any]) -> bool:
        """Пересчёт сигнала на исправленных данных хранилища (блокирующий)."""
        repo = self._repos[name]
        payload = signal["payload"]
        if signal["event_type"] == LARGE_AGGRESSOR:
            second = ev.parse_iso(payload["trade_time"]).replace(microsecond=0)
            ticks = repo.read_ticks(second, second + timedelta(seconds=1))
            return recheck_large_aggressor(payload, ticks, self._instruments[name])
        if signal["event_type"] == VOLUME_SPIKE:
            start = ev.parse_iso(signal["window_from"])
            ticks = repo.read_ticks(start, start + timedelta(minutes=1))
            if ticks.empty:
                ticks = pd.DataFrame(columns=["volume", "direction"])
            return recheck_volume_spike(payload, ticks, signal["direction"])
        return False

    async def _resolve(
        self, signal: dict[str, Any], now: datetime, outcome: str | None, reason: str | None
    ) -> None:
        kind = ev.SIGNAL_CONFIRMED if outcome is not None else ev.SIGNAL_REVOKED
        event = ev.result_event(signal, kind, now, outcome=outcome, reason=reason)
        done = await asyncio.to_thread(
            self._repo.resolve_signal,
            signal["signal_id"],
            STATUS_CONFIRMED if outcome is not None else STATUS_REVOKED,
            outcome,
            reason,
            event["event_id"],
            ev.dumps(event),
            ev.iso(now),
        )
        if not done:
            return
        if outcome is not None:
            logger.info(
                "SIGNAL_CONFIRMED %s %s: %s", signal["ticker"], signal["signal_id"], outcome
            )
        else:
            logger.warning(
                "SIGNAL_REVOKED %s %s: %s", signal["ticker"], signal["signal_id"], reason
            )
