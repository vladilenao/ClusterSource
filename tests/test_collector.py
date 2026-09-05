"""Тесты коллектора: конвертация DTO, обнаружение пропусков trade_id, батч консьюмера."""

from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any

from cluster_source.collector import DataCollector, StreamHandle, Tick, _to_dto
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
