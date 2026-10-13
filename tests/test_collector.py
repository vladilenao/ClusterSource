"""Тесты коллектора: конвертация DTO, пропуски trade_id, приём сделок, батч консьюмера."""

from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any

from cluster_source.collector import DataCollector, _to_dto
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


def test_numeric_trade_id_gap_check_removed() -> None:
    """SPEC-0005 G-4: непрерывность номеров не признак полноты — проверки больше нет."""
    assert not hasattr(DataCollector, "_flag_gap")


async def test_consumer_flushes_batch_and_drains(share_config: AppConfig) -> None:
    """Сделки, принятые deliver, записываются консьюмером при остановке."""
    repo = MarketDataRepository(share_config.storage_root, share_config.instruments[0])
    collector = DataCollector(share_config, share_config.instruments[0], repo)
    for i in range(5):
        await collector.deliver(_trade(100.0 + i, str(i + 1)))
    collector._running = False
    await collector._consumer()
    assert repo.count_hot() == 5


def _fast_config(share_config: AppConfig) -> AppConfig:
    from dataclasses import replace

    from cluster_source.config import BatchConfig

    return replace(share_config, batch=BatchConfig(flush_interval_sec=0.3, batch_size=1000))


async def test_stream_open_marks_day_via_consumer(share_config: AppConfig) -> None:
    """Открытие стрима отмечает день stream_gap; в БД пишет консьюмер (A2)."""
    repo = MarketDataRepository(share_config.storage_root, share_config.instruments[0])
    collector = DataCollector(share_config, share_config.instruments[0], repo)
    collector.on_stream_opened()
    assert repo.days_to_refresh() == {}  # продюсер в БД не пишет
    await collector._flush([])
    today = datetime.now(UTC).strftime("%Y-%m-%d")
    assert repo.days_to_refresh()[today] == "stream_gap"


def test_to_dto_unspecified_direction_is_unknown() -> None:
    """KI-10: TRADE_DIRECTION_UNSPECIFIED не превращается в SELL."""
    instr = Instrument(id="SBER", type=InstrumentType.SHARE, tick_size=0.01, lot_size=1)
    assert _to_dto(_trade(1.0, "1", direction=0), instr).direction == "UNKNOWN"


async def test_cancel_drains_queue(share_config: AppConfig) -> None:
    """KI-08: отмена задачи коллектора не теряет накопленные тики."""
    import asyncio

    cfg = _fast_config(share_config)
    repo = MarketDataRepository(cfg.storage_root, cfg.instruments[0])
    collector = DataCollector(cfg, cfg.instruments[0], repo)
    task = asyncio.create_task(collector.run())
    for i in range(3):
        await collector.deliver(_trade(100.0 + i, str(i)))
    await asyncio.sleep(0)
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass
    assert repo.count_hot() == 3


async def test_shutdown_stops_run_and_drains(share_config: AppConfig) -> None:
    import asyncio

    cfg = _fast_config(share_config)
    repo = MarketDataRepository(cfg.storage_root, cfg.instruments[0])
    collector = DataCollector(cfg, cfg.instruments[0], repo)
    task = asyncio.create_task(collector.run())
    await collector.deliver(_trade(100.0, "1"))
    await collector.shutdown()
    await asyncio.wait_for(task, timeout=5)
    assert repo.count_hot() == 1


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
