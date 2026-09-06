"""Коллектор тиков в реальном времени по схеме Producer-Consumer.

Сбор идут два независимых asyncio-потока:

* Задача 1 (Producer): слушает gRPC-стрим рыночных данных T-Investments,
  преобразует входящие protobuf-объекты в простые DTO/словари и кладёт их
  в :class:`asyncio.Queue`.
* Задача 2 (Consumer): каждые ``flush_interval_sec`` или накопленный
  ``batch_size`` тиков опустошает очередь и выполняет пакетную запись через
  инжектированный репозиторий.

gRPC-стрим никогда не пишется напрямую в базу данных.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from cluster_source.config import AppConfig, Instrument
from cluster_source.database import MarketDataRepository

logger = logging.getLogger(__name__)

MAX_RETRY_DELAY = 60.0
RETRY_BASE = 2.0


class InstrumentNotFound(RuntimeError):
    """Инструмент не найден у провайдера (тикер не существует).

    Коллектор останавливается без повторов, в отличие от временных ошибок
    стрима или резолюции FIGI.
    """


@dataclass(frozen=True)
class StreamHandle:
    """Открытый стрим плюс его callback завершения.

    ``stream`` — async-итерируемый по сырым торговым payload. ``teardown`` —
    async-вызываемый, вызывается ровно один раз, когда сбор для этого
    инструмента заканчивается, чтобы SDK-клиенты/стримы закрывались
    детерминированно.
    """

    stream: Any
    teardown: Callable[[], Awaitable[None]] | None = None


StreamFactory = Callable[[Instrument], Awaitable[StreamHandle]]
"""Инжектируемая зависимость: открывает стрим для инструмента."""


@dataclass(frozen=True)
class Tick:
    """Неизменяемый DTO для одной сделки (тика)."""

    ticker: str
    timestamp: datetime
    price: float
    volume: int
    direction: str  # BUY / SELL
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
    direction_raw = str(getattr(trade, "direction", ""))
    direction = "BUY" if direction_raw in ("1", "BUY", "TRADE_DIRECTION_BUY") else "SELL"
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
        direction=direction,
        trade_id=trade_id,
    )


class DataCollector:
    """Координирует задачи продюсера и консьюмера для одного инструмента."""

    def __init__(
        self,
        config: AppConfig,
        instrument: Instrument,
        repo: MarketDataRepository,
        *,
        stream_factory: StreamFactory | None = None,
    ) -> None:
        self._config = config
        self._instrument = instrument
        self._repo = repo
        self._queue: asyncio.Queue[Tick] = asyncio.Queue(maxsize=10000)
        self._stream_factory = stream_factory
        self._running = False
        self._tasks: list[asyncio.Task[Any]] = []

    async def subscribe(self, stream: AsyncIterator[Any]) -> None:
        """Запускает «неубиваемый» цикл продюсера на данном стриме.

        Отслеживает непрерывность trade_id; при пропуске день помечается,
        чтобы ночной архиватор/автозагрузчик мог перекачать его.
        """
        prev_id_stack: list[str] = []

        try:
            async for item in stream:
                # duck-typing потребитель стрим-итератора
                if not self._running:
                    return
                # t-tech-investments отдаёт обёртки MarketDataResponse;
                # распаковываем саму сделку и пропускаем ping/subscription-фреймы.
                payload: Any = item
                if hasattr(item, "trade"):
                    if item.trade is None:
                        continue
                    payload = item.trade
                tick = _to_dto(payload, self._instrument)
                self._flag_gap(tick, prev_id_stack)
                await self._queue.put(tick)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # pragma: no cover - resilience
            logger.error("Ошибка потока продюсера: %s", exc)

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
        prev_ids.append(tick.trade_id)

    async def _consumer(self) -> None:
        buffer: list[Tick] = []
        interval = self._config.batch.flush_interval_sec
        batch_size = self._config.batch.batch_size

        while self._running or not self._queue.empty():
            try:
                tick = await asyncio.wait_for(self._queue.get(), timeout=interval)
                buffer.append(tick)
            except TimeoutError:
                pass

            should_flush = len(buffer) >= batch_size
            if buffer and (should_flush or self._queue.empty()):
                try:
                    await asyncio.to_thread(
                        self._repo.insert_ticks,
                        [_tick_to_dict(t) for t in buffer],
                    )
                except Exception as exc:  # pragma: no cover - resilience
                    logger.error("Ошибка флаша консьюмера: %s", exc)
                buffer.clear()

        if buffer:
            try:
                await asyncio.to_thread(
                    self._repo.insert_ticks,
                    [_tick_to_dict(t) for t in buffer],
                )
            except Exception as exc:
                logger.error("Ошибка финального флаша консьюмера: %s", exc)

    async def run(self) -> None:
        """Запускает пару продюсер/консьюмер и держит стрим живым.

        При обрыве соединения переподключается с экспоненциальной задержкой
        (2 с .. 60 с). При завершении (``_running`` переведён в False) консьюмер
        опустошает всю очередь, чтобы не потерять ни одного тика.
        """
        self._running = True
        consumer_task = asyncio.create_task(self._consumer())

        delay = 0.0
        while self._running:
            if self._stream_factory is None:
                logger.warning("Не внедрён stream factory для %s", self._instrument.id)
                await asyncio.sleep(RETRY_BASE)
                continue

            try:
                handle = await self._stream_factory(self._instrument)
            except asyncio.CancelledError:
                break
            except InstrumentNotFound as exc:
                logger.warning(
                    "Инструмент %s не найден — запуск коллектора отменён: %s",
                    self._instrument.id,
                    exc,
                )
                break
            except Exception as exc:  # pragma: no cover - resilience
                delay = max(delay, RETRY_BASE)
                logger.error(
                    "Не удалось создать стрим для %s: %s; повтор через %.1f с",
                    self._instrument.id,
                    exc,
                    delay,
                )
                await asyncio.sleep(delay)
                delay = min(delay * 2, MAX_RETRY_DELAY)
                continue

            delay = 0.0
            logger.info(
                "Коллектор запущен для %s (ключ хранения %s)",
                self._instrument.id,
                self._instrument.storage_name,
            )
            try:
                await self.subscribe(handle.stream)
            except asyncio.CancelledError:
                await self._teardown(handle)
                break
            except Exception as exc:  # disconnect / stream error
                logger.warning(
                    "Поток прерван для %s: %s. Переподключение с экспоненциальной задержкой",
                    self._instrument.id,
                    exc,
                )
                await self._teardown(handle)
                await asyncio.sleep(delay)
                delay = max(delay, RETRY_BASE)
                delay = min(delay * 2, MAX_RETRY_DELAY)
                continue

            # Стрим закрыт штатно (например, ночью) — переподключаемся.
            await self._teardown(handle)
            await asyncio.sleep(RETRY_BASE)
            delay = RETRY_BASE

        # Корректное завершение: даём консьюмеру сбросить все оставшиеся тики.
        self._running = False
        try:
            await asyncio.wait_for(consumer_task, timeout=30)
        except TimeoutError:
            consumer_task.cancel()

    @staticmethod
    async def _teardown(handle: StreamHandle) -> None:
        if handle.teardown is not None:
            try:
                await handle.teardown()
            except Exception as exc:  # pragma: no cover - resilience
                logger.warning("Ошибка завершения стрима: %s", exc)

    async def shutdown(self) -> None:
        """Сигнализирует циклу о остановке и освобождает ресурсы стрима."""
        self._running = False


def _tick_to_dict(tick: Tick) -> dict[str, object]:
    return {
        "ticker": tick.ticker,
        "timestamp": tick.timestamp,
        "price": tick.price,
        "volume": tick.volume,
        "direction": tick.direction,
        "trade_id": tick.trade_id,
    }
