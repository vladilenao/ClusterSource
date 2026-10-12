"""Коллектор тиков в реальном времени по схеме Producer-Consumer.

Сбор идут два независимых asyncio-потока:

* Задача 1 (Producer): слушает gRPC-стрим рыночных данных T-Investments,
  преобразует входящие protobuf-объекты в простые DTO/словари и кладёт их
  в :class:`asyncio.Queue`.
* Задача 2 (Consumer): каждые ``flush_interval_sec`` или при накоплении
  ``batch_size`` тиков выполняет пакетную запись через инжектированный
  репозиторий. Консьюмер же сохраняет отметки «день неполный», которые
  продюсер копит в памяти.

gRPC-стрим никогда не пишется напрямую в базу данных.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from cluster_source.config import AppConfig, Instrument, parse_direction
from cluster_source.database import MarketDataRepository

logger = logging.getLogger(__name__)

MAX_RETRY_DELAY = 60.0
RETRY_BASE = 2.0
#: Стрим, проживший дольше, считается стабильным: задержка переподключения сбрасывается.
STABLE_STREAM_SEC = 60.0
CONSUMER_DRAIN_TIMEOUT = 30.0

#: Причины отметки дня для перекачки из официального архива.
REFRESH_COMPROMISED = "compromised"
REFRESH_STREAM_GAP = "stream_gap"


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
        #: Дни, которые нужно перекачать: {YYYY-MM-DD: причина}. Пишет в БД консьюмер.
        self._pending_marks: dict[str, str] = {}
        self._reconnect_requested = False

    async def subscribe(self, stream: AsyncIterator[Any]) -> None:
        """Запускает «неубиваемый» цикл продюсера на данном стриме.

        Отслеживает непрерывность trade_id; при пропуске день помечается,
        чтобы автозагрузчик перекачал его. Ошибки стрима пробрасываются в
        :meth:`run`, который управляет переподключением.
        """
        prev_id_stack: list[str] = []

        async for item in stream:
            # duck-typing потребитель стрим-итератора
            if not self._running or self._reconnect_requested:
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

    @property
    def instrument(self) -> Instrument:
        return self._instrument

    def request_reconnect(self) -> None:
        """Просит переоткрыть стрим (с новой резолюцией инструмента).

        Срабатывает на следующем кадре стрима (сделка или ping сервера).
        Нужен непрерывным фьючерсам: после экспирации активный контракт меняется.
        """
        self._reconnect_requested = True

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
        """Запускает пару продюсер/консьюмер и держит стрим живым.

        Переподключается с экспоненциальной задержкой 2 → 4 → … → 60 с; задержка
        сбрасывается, если стрим проработал не меньше ``STABLE_STREAM_SEC``. При
        остановке или отмене задачи консьюмер дописывает всю очередь (не дольше
        ``CONSUMER_DRAIN_TIMEOUT``), после чего отмена пробрасывается дальше.
        """
        self._running = True
        consumer_task = asyncio.create_task(self._consumer())
        try:
            await self._stream_loop()
        finally:
            self._running = False
            try:
                await asyncio.wait_for(consumer_task, timeout=CONSUMER_DRAIN_TIMEOUT)
            except TimeoutError:
                consumer_task.cancel()

    async def _stream_loop(self) -> None:
        loop = asyncio.get_running_loop()
        delay = 0.0
        while self._running:
            if self._stream_factory is None:
                logger.warning("Не внедрён stream factory для %s", self._instrument.id)
                await asyncio.sleep(RETRY_BASE)
                continue

            self._reconnect_requested = False
            try:
                handle = await self._stream_factory(self._instrument)
            except InstrumentNotFound as exc:
                logger.warning(
                    "Инструмент %s не найден — запуск коллектора отменён: %s",
                    self._instrument.id,
                    exc,
                )
                return
            except Exception as exc:  # pragma: no cover - resilience
                delay = _next_delay(delay)
                logger.error(
                    "Не удалось создать стрим для %s: %s; повтор через %.1f с",
                    self._instrument.id,
                    exc,
                    delay,
                )
                await asyncio.sleep(delay)
                continue

            # Сделки до открытия стрима (начало дня, время переподключения)
            # не получены: день перекачивается из архива, когда тот появится.
            self._mark_day(datetime.now(UTC).strftime("%Y-%m-%d"), REFRESH_STREAM_GAP)
            logger.info(
                "Коллектор запущен для %s (ключ хранения %s)",
                self._instrument.id,
                self._instrument.storage_name,
            )
            started = loop.time()
            try:
                await self.subscribe(handle.stream)
                reason = (
                    "переподключение по запросу"
                    if self._reconnect_requested
                    else "стрим закрыт сервером"
                )
            except Exception as exc:  # disconnect / stream error
                reason = f"ошибка стрима: {exc}"
            finally:
                await self._teardown(handle)

            if not self._running:
                return
            if loop.time() - started >= STABLE_STREAM_SEC:
                delay = 0.0
            delay = _next_delay(delay)
            logger.warning(
                "Поток прерван для %s (%s); переподключение через %.1f с",
                self._instrument.id,
                reason,
                delay,
            )
            await asyncio.sleep(delay)

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


def _next_delay(delay: float) -> float:
    """Следующая задержка переподключения: 2, 4, 8, … не более ``MAX_RETRY_DELAY``."""
    return min(max(delay * 2, RETRY_BASE), MAX_RETRY_DELAY)


def _tick_to_dict(tick: Tick) -> dict[str, object]:
    return {
        "ticker": tick.ticker,
        "timestamp": tick.timestamp,
        "price": tick.price,
        "volume": tick.volume,
        "direction": tick.direction,
        "trade_id": tick.trade_id,
    }
