"""Приём тиков инструмента по схеме Producer-Consumer.

Одно стрим-соединение T-Invest на все инструменты держит
:class:`~cluster_source.stream_hub.StreamHub` (SPEC-0009). Для каждого
инструмента:

* Producer — :meth:`DataCollector.deliver`: хаб передаёт сюда сделки этого
  инструмента; они фильтруются (только биржевые, SPEC-0002), преобразуются в
  :class:`Tick` и кладутся в :class:`asyncio.Queue`.
* Consumer — :meth:`DataCollector.run`: каждые ``flush_interval_sec`` или при
  накоплении ``batch_size`` тиков выполняет пакетную запись через
  инжектированный репозиторий и сохраняет отметки «день неполный», которые
  продюсер копит в памяти.

gRPC-стрим никогда не пишется напрямую в базу данных.
"""

from __future__ import annotations

import asyncio
import logging
import statistics
from collections import deque
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from cluster_source.config import (
    AppConfig,
    Direction,
    Instrument,
    is_exchange_trade,
    parse_direction,
)
from cluster_source.database import MarketDataRepository
from cluster_source.quality import (
    REASON_BACKLOG,
    REASON_DEALER_LEAK,
    REASON_STREAM_SILENCE,
    REASON_UNKNOWN_DIRECTION,
    REASON_WRITE_FAILURE,
    InstrumentQuality,
    is_normal_trading,
    minute_floor,
)

logger = logging.getLogger(__name__)

CONSUMER_DRAIN_TIMEOUT = 30.0

#: Причина отметки дня для перекачки из официального архива. Причина
#: ``compromised`` (разрыв числовых ``trade_id``) больше не ставится — SPEC-0005 G-4.
REFRESH_STREAM_GAP = "stream_gap"

#: Отброшенная дилерская сделка логируется первой и далее каждой N-й.
DEALER_LOG_EVERY = 1000

QUEUE_CAPACITY = 10000
#: SPEC-0004 ``backlog``: начало — очередь заполнена больше чем наполовину, конец — меньше 10%.
BACKLOG_HIGH = QUEUE_CAPACITY // 2
BACKLOG_LOW = QUEUE_CAPACITY // 10
#: SPEC-0004 ``stream_silence``: порог = множитель × 95-й перцентиль интервалов
#: между последними сделками, в пределах [мин, макс]; мало интервалов — максимум.
SILENCE_FACTOR = 20.0
SILENCE_MIN_SEC = 60.0
SILENCE_MAX_SEC = 900.0
SILENCE_MIN_SAMPLES = 20
SILENCE_WINDOW = 200
#: SPEC-0004 ``unknown_direction``: минута с ≥ N сделок и долей без стороны больше порога.
UNKNOWN_MIN_TRADES = 20
UNKNOWN_SHARE = 0.5


class InstrumentNotFound(RuntimeError):
    """Инструмент не найден у провайдера (тикер не существует).

    Инструмент исключается из подписки без повторов, в отличие от временных
    ошибок стрима или резолюции FIGI.
    """


@dataclass(frozen=True)
class Tick:
    """Неизменяемый DTO для одной сделки (тика)."""

    ticker: str
    timestamp: datetime
    price: float
    volume: int
    direction: str  # BUY / SELL / UNKNOWN
    trade_id: str


def _to_dto(
    trade: object,
    instrument: Instrument,
    continuous_resolver: Callable[[Instrument], str] | None = None,
) -> Tick:
    """Преобразует payload сделки T-Investments в :class:`Tick`.

    Duck-typing по payload сделки позволяет юнит-тестам передавать простые
    заглушки, а также поддерживает оба диалекта SDK: ``tinkoff-investments``
    (``price`` как float, ``timestamp`` как секунды/наносекунды, ``tradeId``)
    и новый ``t-tech-investments`` (``price`` как ``Quotation``, ``time`` как
    ``datetime``, в публичном стриме нет ``trade_id``).
    """
    ticker = instrument.storage_name
    ts = getattr(trade, "timestamp", None)
    trade_time = getattr(trade, "time", None)
    if ts is not None:
        timestamp = datetime.fromtimestamp(ts.seconds, tz=UTC)
        timestamp = timestamp.replace(microsecond=ts.nanos // 1000)
    elif isinstance(trade_time, datetime):
        timestamp = trade_time if trade_time.tzinfo else trade_time.replace(tzinfo=UTC)
        timestamp = timestamp.astimezone(UTC)
    else:
        timestamp = datetime.now(UTC)

    price_raw = getattr(trade, "price", 0.0)
    units = getattr(price_raw, "units", None)
    if units is not None:  # Quotation из t-tech-investments
        price = float(units) + float(getattr(price_raw, "nano", 0)) / 1e9
    else:
        price = float(price_raw)
    volume = int(getattr(trade, "quantity", 0))
    direction = parse_direction(getattr(trade, "direction", ""))
    trade_id = str(getattr(trade, "trade_id", "") or getattr(trade, "tradeId", "") or "")
    if not trade_id:
        # Публичный стрим сделок t-tech-investments не содержит trade id;
        # синтезируем нечисловой id. Биржевой номер сделки проставляет сверка
        # с ISS в ``exchange_trade_no`` (SPEC-0005).
        trade_id = f"s{timestamp.strftime('%Y%m%d%H%M%S%f')}"

    return Tick(
        ticker=ticker,
        timestamp=timestamp,
        price=price,
        volume=volume,
        direction=direction.value,
        trade_id=trade_id,
    )


class DataCollector:
    """Приёмник сделок одного инструмента: очередь, консьюмер, отметки дней."""

    def __init__(
        self,
        config: AppConfig,
        instrument: Instrument,
        repo: MarketDataRepository,
        *,
        now: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        self._config = config
        self._instrument = instrument
        self._repo = repo
        self._now = now
        self._queue: asyncio.Queue[Tick] = asyncio.Queue(maxsize=QUEUE_CAPACITY)
        self._running = False
        self._stopped = asyncio.Event()
        #: Дни, которые нужно перекачать: {YYYY-MM-DD: причина}. Пишет в БД консьюмер.
        self._pending_marks: dict[str, str] = {}
        self._dealer_dropped = 0
        #: SPEC-0004: условия деградации и отметки минут; пишет в БД консьюмер.
        self.quality = InstrumentQuality(instrument.storage_name)
        self.trading_status: object = None
        self.trading_status_since: datetime | None = None
        self.last_trade_at: datetime | None = None
        #: С какого момента стрим действительно доставляет сделки инструмента
        #: (подписка подтверждена); ``None`` — не слушаем (разрыв).
        self.listening_since: datetime | None = None
        self._intervals: deque[float] = deque(maxlen=SILENCE_WINDOW)
        self._minute: datetime | None = None
        self._minute_total = 0
        self._minute_unknown = 0
        #: SPEC-0007: ряд открытого интереса из стрима; пишет консьюмер (A2).
        self._pending_oi: list[tuple[datetime, int]] = []
        #: SPEC-0008 G-2: разветвление потока для детекторов сигналов (не блокирует).
        self.tick_listener: Callable[[str, Tick], None] | None = None

    def on_stream_opened(self) -> None:
        """Стрим (пере)открыт: сделки до этого момента не получены.

        День отмечается ``stream_gap`` и будет перекачан из архива.
        """
        self._mark_day(self._now().strftime("%Y-%m-%d"), REFRESH_STREAM_GAP)

    def last_stored_tick_time(self) -> datetime | None:
        """Время последней сохранённой сделки (блокирующий вызов — через ``to_thread``)."""
        return self._repo.last_tick_time()

    async def deliver(self, payload: Any) -> None:
        """Принимает сделку этого инструмента из стрима (продюсер).

        Только кладёт тик в очередь и копит отметки дней в памяти: в БД пишет
        консьюмер (инвариант A2). Блокируется, если очередь заполнена.
        """
        if not is_exchange_trade(getattr(payload, "trade_source", None)):
            self._drop_non_exchange(payload)
            return
        tick = _to_dto(payload, self._instrument)
        self._observe(tick)
        if self.tick_listener is not None:
            self.tick_listener(self._instrument.storage_name, tick)  # SPEC-0008 G-2
        if self._queue.qsize() >= BACKLOG_HIGH:
            self.quality.begin(REASON_BACKLOG, self._now(), self._now())
        await self._queue.put(tick)

    def deliver_open_interest(self, payload: Any) -> None:
        """Сообщение ОИ из стрима (SPEC-0007): только в память, пишет консьюмер."""
        moment = getattr(payload, "time", None)
        if not isinstance(moment, datetime):
            moment = self._now()
        self._pending_oi.append((moment, int(getattr(payload, "open_interest", 0))))

    # ------------------------------------------------------------------ #
    # Детекторы SPEC-0004 на стороне инструмента
    # ------------------------------------------------------------------ #
    def set_trading_status(self, status: object, since: datetime | None) -> None:
        """Торговый статус инструмента из стрима (``SubscribeInfoRequest``)."""
        if status != self.trading_status:
            self.trading_status_since = since or self._now()
        self.trading_status = status

    def silence_threshold(self) -> float:
        """Порог тишины, с: адаптивный по интервалам между последними сделками."""
        if len(self._intervals) < SILENCE_MIN_SAMPLES:
            return SILENCE_MAX_SEC
        p95 = statistics.quantiles(self._intervals, n=20)[-1]
        return min(SILENCE_MAX_SEC, max(SILENCE_MIN_SEC, SILENCE_FACTOR * p95))

    def check_silence(self, now: datetime) -> None:
        """Сторож хаба: тишина при ``NORMAL_TRADING`` дольше порога — деградация (G-4).

        Отсчёт — от последней сделки, перехода в ``NORMAL_TRADING`` или начала
        прослушивания, что позже: время до подписки покрыто ``stream_gap``.
        """
        if self.listening_since is None or not is_normal_trading(self.trading_status):
            return
        candidates = [
            t for t in (self.last_trade_at, self.trading_status_since, self.listening_since) if t
        ]
        last_activity = max(candidates)
        if (now - last_activity).total_seconds() > self.silence_threshold():
            self.quality.begin(REASON_STREAM_SILENCE, last_activity, now)

    def close_minutes(self, now: datetime) -> None:
        """Оценивает закрывшуюся минуту, если сделок в новой минуте ещё не было."""
        if self._minute is not None and minute_floor(now) > self._minute:
            self._close_minute()
            self._minute = None

    def _observe(self, tick: Tick) -> None:
        received = self._now()
        if self.last_trade_at is not None:
            self._intervals.append(max(0.0, (received - self.last_trade_at).total_seconds()))
        self.last_trade_at = received
        if self.quality.is_active(REASON_STREAM_SILENCE):
            self.quality.end(REASON_STREAM_SILENCE, received)
        minute = minute_floor(tick.timestamp)
        if self._minute is not None and minute != self._minute:
            self._close_minute()
        if self._minute != minute:
            self._minute, self._minute_total, self._minute_unknown = minute, 0, 0
        self._minute_total += 1
        if tick.direction == Direction.UNKNOWN.value:
            self._minute_unknown += 1

    def _close_minute(self) -> None:
        if (
            self._minute is not None
            and is_normal_trading(self.trading_status)
            and self._minute_total >= UNKNOWN_MIN_TRADES
            and self._minute_unknown / self._minute_total > UNKNOWN_SHARE
        ):
            self.quality.mark(REASON_UNKNOWN_DIRECTION, self._minute)

    @property
    def instrument(self) -> Instrument:
        return self._instrument

    @property
    def dealer_trades_dropped(self) -> int:
        """Сколько небиржевых сделок пришло вопреки фильтру подписки и было отброшено."""
        return self._dealer_dropped

    def _drop_non_exchange(self, payload: Any) -> None:
        # SPEC-0002 G-3: небиржевая сделка не сохраняется, а учитывается как аномалия.
        self._dealer_dropped += 1
        self.quality.mark(REASON_DEALER_LEAK, self._now())  # SPEC-0004
        if self._dealer_dropped == 1 or self._dealer_dropped % DEALER_LOG_EVERY == 0:
            logger.warning(
                "DEALER_TRADE_DROPPED по %s: источник %s, время %s (всего отброшено %d)",
                self._instrument.id,
                getattr(payload, "trade_source", None),
                getattr(payload, "time", None),
                self._dealer_dropped,
            )

    def _mark_day(self, day: str, reason: str) -> None:
        self._pending_marks.setdefault(day, reason)

    async def _consumer(self) -> None:
        """Пишет тики пакетами: при ``batch_size`` тиков или раз в ``flush_interval_sec``.

        Работает, пока коллектор запущен или в очереди есть тики, поэтому при
        остановке очередь дописывается полностью.
        """
        buffer: list[Tick] = []
        interval = self._config.batch.flush_interval_sec
        batch_size = self._config.batch.batch_size
        loop = asyncio.get_running_loop()
        deadline = loop.time() + interval

        while self._running or not self._queue.empty():
            timeout = deadline - loop.time()
            try:
                if timeout > 0:
                    buffer.append(await asyncio.wait_for(self._queue.get(), timeout=timeout))
                else:
                    buffer.append(self._queue.get_nowait())
            except (TimeoutError, asyncio.QueueEmpty):
                pass

            now = loop.time()
            if len(buffer) >= batch_size or now >= deadline:
                await self._flush(buffer)
                buffer.clear()
                deadline = now + interval

        await self._flush(buffer)

    async def _flush(self, buffer: list[Tick]) -> None:
        if buffer:
            try:
                await asyncio.to_thread(
                    self._repo.insert_ticks,
                    [_tick_to_dict(t) for t in buffer],
                )
                self.quality.end(REASON_WRITE_FAILURE, self._now())
            except Exception as exc:
                logger.error("Ошибка флаша консьюмера: %s", exc)
                # SPEC-0004: пакет потерян — его минуты и время до успешной записи DEGRADED.
                first = min(t.timestamp for t in buffer)
                self.quality.mark(REASON_WRITE_FAILURE, first, max(t.timestamp for t in buffer))
                self.quality.begin(REASON_WRITE_FAILURE, first, self._now())
        if self._pending_oi:
            oi, self._pending_oi = self._pending_oi, []
            try:
                await asyncio.to_thread(self._repo.insert_open_interest, oi)
            except Exception as exc:
                logger.error("Ошибка записи открытого интереса: %s", exc)
                self._pending_oi[:0] = oi
        if self._queue.qsize() <= BACKLOG_LOW:
            self.quality.end(REASON_BACKLOG, self._now())
        minutes, events = self.quality.drain()
        if minutes or events:
            try:
                await asyncio.to_thread(self._repo.save_quality, minutes, events)
            except Exception as exc:
                logger.error("Ошибка сохранения отметок качества: %s", exc)
                self.quality.restore(minutes, events)  # SPEC-0004 G-3
        if self._pending_marks:
            marks, self._pending_marks = self._pending_marks, {}
            try:
                for reason in set(marks.values()):
                    days = [day for day, r in marks.items() if r == reason]
                    await asyncio.to_thread(self._repo.mark_days_for_refresh, days, reason)
            except Exception as exc:  # pragma: no cover - resilience
                logger.error("Ошибка сохранения отметок дней: %s", exc)
                for day, reason in marks.items():
                    self._pending_marks.setdefault(day, reason)

    async def run(self) -> None:
        """Работает консьюмер до :meth:`shutdown` или отмены задачи.

        При остановке или отмене консьюмер дописывает всю очередь (не дольше
        ``CONSUMER_DRAIN_TIMEOUT``), после чего отмена пробрасывается дальше.
        """
        self._running = True
        consumer_task = asyncio.create_task(self._consumer())
        try:
            await self._stopped.wait()
        finally:
            self._running = False
            try:
                await asyncio.wait_for(consumer_task, timeout=CONSUMER_DRAIN_TIMEOUT)
            except TimeoutError:
                consumer_task.cancel()

    async def shutdown(self) -> None:
        """Останавливает консьюмер; накопленные тики будут дописаны."""
        self._running = False
        self._stopped.set()


def _tick_to_dict(tick: Tick) -> dict[str, object]:
    return {
        "ticker": tick.ticker,
        "timestamp": tick.timestamp,
        "price": tick.price,
        "volume": tick.volume,
        "direction": tick.direction,
        "trade_id": tick.trade_id,
    }
