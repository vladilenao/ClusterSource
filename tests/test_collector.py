"""Тесты коллектора: конвертация DTO, обнаружение пропусков trade_id, батч консьюмера."""

from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any

from cluster_source.collector import DataCollector, InstrumentNotFound, StreamHandle, Tick, _to_dto
from cluster_source.config import AppConfig, Instrument, InstrumentType
from cluster_source.database import MarketDataRepository

UTC = UTC


def _trade(price: float, tid: str, direction: Any = "1", quantity: int = 3) -> Any:
    return SimpleNamespace(
        price=price,
        quantity=quantity,
        direction=direction,
        tradeId=tid,
        timestamp=SimpleNamespace(
            seconds=int(datetime.now(UTC).timestamp()),
            nanos=0,
        ),
    )


def test_to_dto_buy_conversion() -> None:
    instr = Instrument(id="SBER", type=InstrumentType.SHARE, tick_size=0.01, lot_size=1)
    tick = _to_dto(_trade(312.5, "42", direction="1", quantity=5), instr)
    assert tick.price == 312.5
    assert tick.volume == 5
    assert tick.direction == "BUY"
    assert tick.trade_id == "42"
    assert tick.ticker == "SBER"
    assert tick.timestamp.tzinfo == UTC


def test_to_dto_sell_and_continuous_storage_key() -> None:
    instr = Instrument(id="Si", type=InstrumentType.CONTINUOUS_FUTURES, tick_size=1.0, lot_size=1)
    tick = _to_dto(_trade(91.0, "7", direction="SELL"), instr)
    assert tick.direction == "SELL"
    assert tick.ticker == "Si_CONTINUOUS"


def test_to_dto_t_tech_sdk_shape() -> None:
    """Trade из t-tech-investments: цена Quotation, время datetime,
    направление enum, без trade_id."""
    instr = Instrument(id="SBER", type=InstrumentType.SHARE, tick_size=0.01, lot_size=1)
    trade = AnyNamedTrade(
        time=datetime(2026, 9, 5, 10, 30, 15, 250000),
        price=PriceQuotation(312, 500000000),
        quantity=4,
        direction=2,
    )
    tick = _to_dto(trade, instr)
    assert tick.price == 312.5
    assert tick.volume == 4
    assert tick.direction == "SELL"
    assert tick.timestamp == datetime(2026, 9, 5, 10, 30, 15, 250000, tzinfo=UTC)
    assert tick.trade_id.startswith("s")  # синтезированный, нечисловой


class PriceQuotation:
    def __init__(self, units: int, nano: int) -> None:
        self.units = units
        self.nano = nano


class AnyNamedTrade:
    def __init__(
        self,
        time: datetime,
        price: PriceQuotation,
        quantity: int,
        direction: int,
    ) -> None:
        self.time = time
        self.price = price
        self.quantity = quantity
        self.direction = direction


def test_gap_detection_logs_warning(
    caplog, share_config: AppConfig, share_repo: MarketDataRepository
) -> None:
    import logging

    collector = DataCollector(share_config, share_repo.instrument, share_repo)
    now = datetime.now(UTC)
    with caplog.at_level(logging.WARNING, logger="cluster_source.collector"):
        collector._flag_gap(Tick("SBER", now, 1.0, 1, "BUY", "10"), ["9"])
        # 10 идёт после 9 -> пропуска нет
        collector._flag_gap(Tick("SBER", now, 1.0, 1, "BUY", "20"), ["10"])
    assert "TRADE_ID GAP" in caplog.text
    assert "COMPROMISED" in caplog.text


async def test_consumer_flushes_batch_and_drains(share_config: AppConfig) -> None:
    repo = MarketDataRepository(share_config.storage_root, share_config.instruments[0])
    collector = DataCollector(share_config, share_config.instruments[0], repo)
    datetime.now(UTC).replace(hour=12, minute=0, second=0, microsecond=0)

    async def fake_factory(_instr: Instrument) -> StreamHandle:
        class FakeStream:
            def __init__(self) -> None:
                self._trades = [_trade(100.0 + i, str(i + 1)) for i in range(5)]
                self._idx = 0

            def __aiter__(self) -> FakeStream:
                return self

            async def __anext__(self) -> Any:
                if self._idx >= len(self._trades):
                    raise StopAsyncIteration
                t = self._trades[self._idx]
                self._idx += 1
                return t

        return StreamHandle(stream=FakeStream(), teardown=None)

    collector._stream_factory = fake_factory
    collector._running = True
    handle = await fake_factory(share_config.instruments[0])
    await collector.subscribe(handle.stream)
    # Конец стрима: выключаем running, чтобы консьюмер опустошил очередь.
    collector._running = False
    await collector._consumer()
    assert repo.count_hot() == 5


async def test_not_found_instrument_stops_without_retry(share_config: AppConfig) -> None:
    repo = MarketDataRepository(share_config.storage_root, share_config.instruments[0])
    collector = DataCollector(share_config, share_config.instruments[0], repo)

    calls = 0

    async def not_found_factory(_instr: Instrument) -> StreamHandle:
        nonlocal calls
        calls += 1
        raise InstrumentNotFound(f"Тикер не найден: {_instr.id}")

    collector._stream_factory = not_found_factory
    await collector.run()
    assert calls == 1
    assert not collector._running
    assert repo.count_hot() == 0


def _fast_config(share_config: AppConfig) -> AppConfig:
    from dataclasses import replace

    from cluster_source.config import BatchConfig

    return replace(share_config, batch=BatchConfig(flush_interval_sec=0.3, batch_size=1000))


async def test_gap_and_stream_open_mark_days_via_consumer(share_config: AppConfig) -> None:
    """KI-03: разрыв trade_id и открытие стрима сохраняют отметку дня (пишет консьюмер)."""
    repo = MarketDataRepository(share_config.storage_root, share_config.instruments[0])
    collector = DataCollector(share_config, share_config.instruments[0], repo)
    day = datetime(2026, 9, 4, 10, tzinfo=UTC)
    collector._flag_gap(Tick("SBER", day, 1.0, 1, "BUY", "20"), ["10"])
    assert repo.days_to_refresh() == {}  # продюсер в БД не пишет
    await collector._flush([])
    assert repo.days_to_refresh() == {"2026-09-04": "compromised"}


def test_to_dto_unspecified_direction_is_unknown() -> None:
    """KI-10: TRADE_DIRECTION_UNSPECIFIED не превращается в SELL."""
    instr = Instrument(id="SBER", type=InstrumentType.SHARE, tick_size=0.01, lot_size=1)
    assert _to_dto(_trade(1.0, "1", direction=0), instr).direction == "UNKNOWN"


def test_next_delay_is_exponential() -> None:
    from cluster_source.collector import _next_delay

    delays = [0.0]
    for _ in range(7):
        delays.append(_next_delay(delays[-1]))
    assert delays[1:] == [2.0, 4.0, 8.0, 16.0, 32.0, 60.0, 60.0]


class _ListStream:
    """Стрим из готовых сделок; по окончании — ошибка соединения."""

    def __init__(self, trades: list[Any]) -> None:
        self._trades = list(trades)

    def __aiter__(self) -> _ListStream:
        return self

    async def __anext__(self) -> Any:
        if self._trades:
            return self._trades.pop(0)
        raise ConnectionError("stream reset")


async def test_stream_errors_back_off_exponentially(share_config: AppConfig, monkeypatch) -> None:
    """KI-07: повторяющиеся разрывы стрима дают растущую задержку."""
    import asyncio

    cfg = _fast_config(share_config)
    repo = MarketDataRepository(cfg.storage_root, cfg.instruments[0])
    collector = DataCollector(cfg, cfg.instruments[0], repo)
    real_sleep = asyncio.sleep
    delays: list[float] = []

    async def fake_sleep(delay: float, *args: Any) -> None:
        delays.append(delay)
        if len(delays) >= 3:
            collector._running = False
        await real_sleep(0)

    async def factory(_instr: Instrument) -> StreamHandle:
        return StreamHandle(stream=_ListStream([]))

    monkeypatch.setattr(asyncio, "sleep", fake_sleep)
    collector._stream_factory = factory
    await collector.run()
    assert delays[:3] == [2.0, 4.0, 8.0]
    today = datetime.now(UTC).strftime("%Y-%m-%d")
    assert repo.days_to_refresh() == {today: "stream_gap"}


async def test_cancel_during_backoff_drains_queue(share_config: AppConfig) -> None:
    """KI-08: отмена во время паузы переподключения не теряет накопленные тики."""
    import asyncio

    cfg = _fast_config(share_config)
    repo = MarketDataRepository(cfg.storage_root, cfg.instruments[0])
    collector = DataCollector(cfg, cfg.instruments[0], repo)

    async def factory(_instr: Instrument) -> StreamHandle:
        return StreamHandle(stream=_ListStream([_trade(100.0 + i, str(i)) for i in range(3)]))

    collector._stream_factory = factory
    task = asyncio.create_task(collector.run())
    await asyncio.sleep(0.05)  # тики в буфере, run спит перед переподключением
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass
    assert repo.count_hot() == 3


async def test_consumer_batches_by_interval(share_config: AppConfig) -> None:
    """KI-06: тики, пришедшие в пределах flush_interval_sec, пишутся одной транзакцией."""
    import asyncio

    cfg = _fast_config(share_config)
    repo = MarketDataRepository(cfg.storage_root, cfg.instruments[0])
    collector = DataCollector(cfg, cfg.instruments[0], repo)
    calls: list[int] = []
    original = repo.insert_ticks

    def counting_insert(rows: Any) -> None:
        calls.append(len(rows))
        original(rows)

    repo.insert_ticks = counting_insert  # type: ignore[method-assign]
    collector._running = True
    consumer = asyncio.create_task(collector._consumer())
    for i in range(5):
        await collector._queue.put(_to_dto(_trade(100.0, str(i)), cfg.instruments[0]))
        await asyncio.sleep(0.01)
    collector._running = False
    await consumer
    assert calls == [5]


async def test_request_reconnect_reopens_stream(share_config: AppConfig, monkeypatch) -> None:
    """После запроса стрим переоткрывается через фабрику (новая резолюция контракта)."""
    import asyncio

    cfg = _fast_config(share_config)
    repo = MarketDataRepository(cfg.storage_root, cfg.instruments[0])
    collector = DataCollector(cfg, cfg.instruments[0], repo)
    real_sleep = asyncio.sleep
    opened = 0

    class PingStream:
        def __aiter__(self) -> PingStream:
            return self

        async def __anext__(self) -> Any:
            await real_sleep(0.01)
            return SimpleNamespace(trade=None)  # ping-кадр

    async def factory(_instr: Instrument) -> StreamHandle:
        nonlocal opened
        opened += 1
        if opened == 1:
            collector.request_reconnect()
        else:
            collector._running = False
        return StreamHandle(stream=PingStream())

    async def fast_sleep(_delay: float, *args: Any) -> None:
        await real_sleep(0)

    monkeypatch.setattr(asyncio, "sleep", fast_sleep)
    collector._stream_factory = factory
    await asyncio.wait_for(collector.run(), timeout=5)
    assert opened == 2
