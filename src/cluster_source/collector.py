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
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from cluster_source.config import AppConfig, Instrument, is_exchange_trade, parse_direction
from cluster_source.database import MarketDataRepository

logger = logging.getLogger(__name__)

CONSUMER_DRAIN_TIMEOUT = 30.0

#: Причины отметки дня для перекачки из официального архива.
REFRESH_COMPROMISED = "compromised"
REFRESH_STREAM_GAP = "stream_gap"

#: Отброшенная дилерская сделка логируется первой и далее каждой N-й.
DEALER_LOG_EVERY = 1000


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
        # Публичный стрим сделок t-tech-investments не содержит
        # последовательного trade id; синтезируем монотонный нечисловой id,
        # чтобы числовая проверка пропусков в _flag_gap безопасно пропускалась.
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
        self, config: AppConfig, instrument: Instrument, repo: MarketDataRepository
    ) -> None:
        self._config = config
        self._instrument = instrument
        self._repo = repo
        self._queue: asyncio.Queue[Tick] = asyncio.Queue(maxsize=10000)
        self._running = False
        self._stopped = asyncio.Event()
        #: Дни, которые нужно перекачать: {YYYY-MM-DD: причина}. Пишет в БД консьюмер.
        self._pending_marks: dict[str, str] = {}
        self._prev_ids: list[str] = []
        self._dealer_dropped = 0

    def on_stream_opened(self) -> None:
        """Стрим (пере)открыт: сделки до этого момента не получены.

        День отмечается ``stream_gap`` и будет перекачан из архива; контроль
        непрерывности ``trade_id`` начинается заново.
        """
        self._mark_day(datetime.now(UTC).strftime("%Y-%m-%d"), REFRESH_STREAM_GAP)
        self._prev_ids = []

    async def deliver(self, payload: Any) -> None:
        """Принимает сделку этого инструмента из стрима (продюсер).

        Только кладёт тик в очередь и копит отметки дней в памяти: в БД пишет
        консьюмер (инвариант A2). Блокируется, если очередь заполнена.
        """
        if not is_exchange_trade(getattr(payload, "trade_source", None)):
            self._drop_non_exchange(payload)
            return
        tick = _to_dto(payload, self._instrument)
        self._flag_gap(tick, self._prev_ids)
        await self._queue.put(tick)

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

    def _flag_gap(self, tick: Tick, prev_ids: list[str]) -> None:
        if not tick.trade_id or not prev_ids:
            prev_ids.append(tick.trade_id)
            return
        try:
            prev = int(prev_ids[-1])
            cur = int(tick.trade_id)
        except (TypeError, ValueError):
            prev_ids.append(tick.trade_id)
            return
        if cur > prev + 1:
            day = tick.timestamp.strftime("%Y-%m-%d")
            logger.warning(
                "TRADE_ID GAP по %s (%s): ожидалось около %d, получено %d"
                " — день %s помечен COMPROMISED",
                self._instrument.id,
                tick.ticker,
                prev + 1,
                cur,
                day,
            )
            self._mark_day(day, REFRESH_COMPROMISED)
        prev_ids.append(tick.trade_id)

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
            except Exception as exc:  # pragma: no cover - resilience
                logger.error("Ошибка флаша консьюмера: %s", exc)
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
