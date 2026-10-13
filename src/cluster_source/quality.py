"""Качество данных инструмента: условия деградации и отметки минут (SPEC-0004).

Детекторы (в хабе и коллекторе) сообщают о проблемах через
:class:`InstrumentQuality`:

* **условие** (:meth:`InstrumentQuality.begin` / :meth:`end`) — длящаяся
  проблема с причиной и моментом начала; пока оно активно, инструмент в
  состоянии ``DEGRADED``, а каждая минута от начала до конца получает отметку
  с этой причиной;
* **разовая отметка** (:meth:`InstrumentQuality.mark`) — одна минута без
  смены состояния.

Объект только копит отметки и переходы в памяти; в БД их пишет консьюмер
(:meth:`drain`), продюсер к хранилищу не обращается (инвариант A2, G-3).
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime, timedelta

logger = logging.getLogger(__name__)

STATE_OK = "OK"
STATE_DEGRADED = "DEGRADED"

REASON_STREAM_GAP = "stream_gap"
REASON_SUBSCRIPTION_FAILED = "subscription_failed"
REASON_STREAM_SILENCE = "stream_silence"
REASON_LATENCY = "latency"
REASON_BACKLOG = "backlog"
REASON_WRITE_FAILURE = "write_failure"
REASON_UNKNOWN_DIRECTION = "unknown_direction"
REASON_CONTRACT_ROLL = "contract_roll"
REASON_DEALER_LEAK = "dealer_leak"

#: ``SECURITY_TRADING_STATUS_NORMAL_TRADING`` — единственный статус, при котором
#: отсутствие сделок подозрительно (G-4).
NORMAL_TRADING = 5

MinuteMark = tuple[str, str]  # (начало минуты ISO UTC, причина)
QualityEvent = tuple[str, str, str]  # (момент ISO UTC, состояние, причины)


def is_normal_trading(status: object) -> bool:
    """Статус ``NORMAL_TRADING`` (SDK — ``IntEnum``, допускается и имя)."""
    if isinstance(status, int):
        return int(status) == NORMAL_TRADING
    return str(status).strip().upper() in {"SECURITY_TRADING_STATUS_NORMAL_TRADING", "5"}


def minute_floor(moment: datetime) -> datetime:
    moment = moment if moment.tzinfo else moment.replace(tzinfo=UTC)
    return moment.astimezone(UTC).replace(second=0, microsecond=0)


def minute_key(moment: datetime) -> str:
    return minute_floor(moment).isoformat()


def minutes_between(start: datetime, end: datetime) -> list[datetime]:
    """Начала минут, пересекающих ``[start, end]`` (обе границы включительно)."""
    first, last = minute_floor(start), minute_floor(end)
    out: list[datetime] = []
    cur = first
    while cur <= last:
        out.append(cur)
        cur += timedelta(minutes=1)
    return out


class InstrumentQuality:
    """Условия деградации и отметки минут одного инструмента."""

    def __init__(self, name: str) -> None:
        self.name = name
        self._conditions: dict[str, datetime] = {}
        self._marked_upto: dict[str, datetime] = {}
        self._pending_minutes: set[MinuteMark] = set()
        self._pending_events: list[QualityEvent] = []

    @property
    def state(self) -> str:
        return STATE_DEGRADED if self._conditions else STATE_OK

    @property
    def reasons(self) -> list[str]:
        return sorted(self._conditions)

    def is_active(self, reason: str) -> bool:
        return reason in self._conditions

    def since(self, reason: str) -> datetime | None:
        return self._conditions.get(reason)

    def begin(self, reason: str, since: datetime, now: datetime) -> None:
        """Начинает условие (повторный вызов при активном условии ничего не меняет)."""
        if reason in self._conditions:
            self._mark_range(reason, self._marked_upto[reason], now)
            return
        since = min(since, now)
        self._conditions[reason] = since
        self._marked_upto[reason] = since
        self._mark_range(reason, since, now)
        self._transition(now)

    def end(self, reason: str, now: datetime) -> None:
        """Завершает условие; минуты до ``now`` включительно остаются помеченными."""
        if reason not in self._conditions:
            return
        self._mark_range(reason, self._marked_upto[reason], now)
        del self._conditions[reason]
        del self._marked_upto[reason]
        self._transition(now)

    def mark(self, reason: str, start: datetime, end: datetime | None = None) -> None:
        """Разовая отметка минут ``[start, end]`` без смены состояния."""
        for minute in minutes_between(start, end or start):
            self._pending_minutes.add((minute.isoformat(), reason))

    def tick(self, now: datetime) -> None:
        """Помечает текущие минуты всех активных условий (вызывает сторож хаба)."""
        for reason in self._conditions:
            self._mark_range(reason, self._marked_upto[reason], now)

    def _mark_range(self, reason: str, start: datetime, end: datetime) -> None:
        self.mark(reason, start, end)
        self._marked_upto[reason] = max(self._marked_upto.get(reason, end), minute_floor(end))

    def _transition(self, now: datetime) -> None:
        reasons = ",".join(self.reasons)
        self._pending_events.append((now.astimezone(UTC).isoformat(), self.state, reasons))
        if self._conditions:
            logger.warning("DATA_DEGRADED %s: %s", self.name, reasons)
        else:
            logger.info("DATA_RESTORED %s", self.name)

    def drain(self) -> tuple[list[MinuteMark], list[QualityEvent]]:
        """Забирает накопленные отметки и переходы для записи консьюмером."""
        minutes = sorted(self._pending_minutes)
        events = list(self._pending_events)
        self._pending_minutes.clear()
        self._pending_events.clear()
        return minutes, events

    def restore(self, minutes: list[MinuteMark], events: list[QualityEvent]) -> None:
        """Возвращает незаписанные отметки (ошибка записи) — G-3."""
        self._pending_minutes.update(minutes)
        self._pending_events[:0] = events
