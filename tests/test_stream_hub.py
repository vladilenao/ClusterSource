"""SPEC-0009: одно стрим-соединение на все инструменты."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Sequence
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any

import pytest

from cluster_source import stream_hub
from cluster_source.collector import DataCollector
from cluster_source.config import AppConfig, Instrument, InstrumentType
from cluster_source.database import MarketDataRepository
from cluster_source.stream_hub import HubConnection, StreamHub, _next_delay, subscription_ok

UTC = UTC
SBER = Instrument(id="SBER", type=InstrumentType.SHARE, tick_size=0.01, lot_size=1)
GAZP = Instrument(id="GAZP", type=InstrumentType.SHARE, tick_size=0.01, lot_size=1)
SI = Instrument(id="Si", type=InstrumentType.CONTINUOUS_FUTURES, tick_size=1, lot_size=1)
ROUTES = {"FIGI_SBER": "SBER", "FIGI_GAZP": "GAZP", "FIGI_SIZ6": "Si_CONTINUOUS"}


def trade(figi: str, price: float = 100.0, uid: str = "") -> Any:
    return SimpleNamespace(
        figi=figi,
        instrument_uid=uid,
        price=price,
        quantity=1,
        direction=1,
        time=datetime.now(UTC),
        trade_source=1,
    )


def frame(**payload: Any) -> Any:
    base: dict[str, Any] = {"trade": None, "subscribe_trades_response": None}
    base.update(payload)
    return SimpleNamespace(**base)


class ListStream:
    """Кадры по очереди; затем — обрыв соединения или бесконечные ping-кадры."""

    def __init__(self, frames: list[Any], then_ping: bool = False) -> None:
        self._frames = list(frames)
        self._then_ping = then_ping

    def __aiter__(self) -> ListStream:
        return self

    async def __anext__(self) -> Any:
        if self._frames:
            return self._frames.pop(0)
        if self._then_ping:
            await _REAL_SLEEP(0.001)
            return frame()
        raise ConnectionError("stream reset")


_REAL_SLEEP = asyncio.sleep


@pytest.fixture
def collectors(share_config: AppConfig) -> dict[str, DataCollector]:
    out = {}
    for instr in (SBER, GAZP, SI):
        repo = MarketDataRepository(share_config.storage_root, instr)
        out[instr.storage_name] = DataCollector(share_config, instr, repo)
    return out


def queued(collector: DataCollector) -> list[float]:
    return [t.price for t in list(collector._queue._queue)]  # type: ignore[attr-defined]


class Factory:
    """Фабрика соединений: выдаёт заготовленные стримы и запоминает вызовы."""

    def __init__(self, streams: list[Any], hub_ref: list[StreamHub] | None = None) -> None:
        self.streams = list(streams)
        self.calls: list[tuple[list[str], bool]] = []
        self.teardowns = 0
        self.hub_ref = hub_ref

    async def __call__(self, instruments: Sequence[Instrument], resolve: bool) -> HubConnection:
        self.calls.append(([i.storage_name for i in instruments], resolve))
        if not self.streams:
            if self.hub_ref:
                await self.hub_ref[0].shutdown()
            return HubConnection(stream=ListStream([]), routes=ROUTES)
        item = self.streams.pop(0)
        if isinstance(item, Exception):
            raise item

        async def teardown() -> None:
            self.teardowns += 1

        return HubConnection(stream=item, routes=ROUTES, teardown=teardown)


@pytest.fixture
def fast_sleep(monkeypatch: pytest.MonkeyPatch) -> list[float]:
    delays: list[float] = []

    async def fake_sleep(delay: float, *args: Any) -> None:
        delays.append(delay)
        await _REAL_SLEEP(0)

    monkeypatch.setattr(stream_hub.asyncio, "sleep", fake_sleep)
    return delays


async def _run(hub: StreamHub) -> None:
    await asyncio.wait_for(hub.run(), timeout=5)


# --------------------------------------------------------------------------- #
async def test_one_connection_routes_trades_to_instruments(
    collectors: dict[str, DataCollector], fast_sleep: list[float]
) -> None:
    """SPEC-0009 A-1, A-2: одно соединение на все инструменты, сделки — по своим очередям."""
    hub_ref: list[StreamHub] = []
    stream = ListStream(
        [
            frame(trade=trade("FIGI_SBER", 1.0)),
            frame(trade=trade("FIGI_GAZP", 2.0)),
            frame(trade=trade("", 3.0, uid="FIGI_SIZ6")),  # маршрут по instrument_uid
            frame(trade=trade("FIGI_SBER", 4.0)),
        ]
    )
    factory = Factory([stream], hub_ref)
    hub = StreamHub(list(collectors.values()), factory)
    hub_ref.append(hub)
    await _run(hub)

    assert factory.calls[0] == (["SBER", "GAZP", "Si_CONTINUOUS"], True)
    assert queued(collectors["SBER"]) == [1.0, 4.0]
    assert queued(collectors["GAZP"]) == [2.0]
    assert queued(collectors["Si_CONTINUOUS"]) == [3.0]
    assert factory.teardowns == 1


async def test_unknown_instrument_is_logged_once(
    collectors: dict[str, DataCollector],
    fast_sleep: list[float],
    caplog: pytest.LogCaptureFixture,
) -> None:
    """SPEC-0009 A-2: сделка по неподписанному FIGI пропускается, предупреждение — один раз."""
    hub_ref: list[StreamHub] = []
    stream = ListStream([frame(trade=trade("FIGI_X")), frame(trade=trade("FIGI_X"))])
    hub = StreamHub(list(collectors.values()), Factory([stream], hub_ref))
    hub_ref.append(hub)
    with caplog.at_level(logging.WARNING):
        await _run(hub)
    assert caplog.text.count("неподписанному инструменту FIGI_X") == 1
    assert all(not queued(c) for c in collectors.values())


async def test_reconnect_resubscribes_all_and_marks_gap(
    collectors: dict[str, DataCollector], fast_sleep: list[float]
) -> None:
    """SPEC-0009 A-3, G-4: обрыв — переподключение без новой резолюции FIGI, stream_gap у всех."""
    hub_ref: list[StreamHub] = []
    factory = Factory([ListStream([]), ListStream([]), ListStream([])], hub_ref)
    hub = StreamHub(list(collectors.values()), factory)
    hub_ref.append(hub)
    await _run(hub)

    assert [resolve for _, resolve in factory.calls[:3]] == [True, False, False]
    assert fast_sleep[:3] == [2.0, 4.0, 8.0]
    today = datetime.now(UTC).strftime("%Y-%m-%d")
    for collector in collectors.values():
        assert collector._pending_marks == {today: "stream_gap"}


async def test_factory_errors_back_off_and_keep_resolving(
    collectors: dict[str, DataCollector], fast_sleep: list[float]
) -> None:
    """SPEC-0009: ошибка открытия — экспоненциальная пауза; резолюция повторяется до успеха."""
    hub_ref: list[StreamHub] = []
    factory = Factory([ConnectionError("dns"), ConnectionError("dns"), ListStream([])], hub_ref)
    hub = StreamHub(list(collectors.values()), factory)
    hub_ref.append(hub)
    await _run(hub)
    assert [resolve for _, resolve in factory.calls[:4]] == [True, True, True, False]
    assert fast_sleep[:2] == [2.0, 4.0]


async def test_failed_subscription_is_reported(
    collectors: dict[str, DataCollector],
    fast_sleep: list[float],
    caplog: pytest.LogCaptureFixture,
) -> None:
    """SPEC-0009 A-4 (G-3): неподтверждённая подписка — STREAM_SUBSCRIPTION_FAILED."""
    response = SimpleNamespace(
        trade_subscriptions=[
            SimpleNamespace(figi="FIGI_SBER", subscription_status=1),
            SimpleNamespace(figi="FIGI_GAZP", subscription_status=6),  # LIMIT_IS_EXCEEDED
        ]
    )
    hub_ref: list[StreamHub] = []
    hub = StreamHub(
        list(collectors.values()),
        Factory([ListStream([frame(subscribe_trades_response=response)])], hub_ref),
    )
    hub_ref.append(hub)
    with caplog.at_level(logging.ERROR):
        await _run(hub)
    assert "STREAM_SUBSCRIPTION_FAILED GAZP" in caplog.text
    assert "STREAM_SUBSCRIPTION_FAILED SBER" not in caplog.text


async def test_request_reconnect_with_resolve(
    collectors: dict[str, DataCollector], fast_sleep: list[float]
) -> None:
    """SPEC-0009 A-5: ролловер — переподключение сразу, с новой резолюцией FIGI."""
    hub_ref: list[StreamHub] = []
    factory = Factory([ListStream([], then_ping=True), ListStream([])], hub_ref)
    hub = StreamHub(list(collectors.values()), factory)
    hub_ref.append(hub)

    async def roll() -> None:
        while len(factory.calls) < 1:
            await _REAL_SLEEP(0.001)
        hub.request_reconnect(resolve=True)

    await asyncio.gather(_run(hub), roll())
    assert [resolve for _, resolve in factory.calls[:2]] == [True, True]
    # Переоткрытие по запросу — без паузы; единственная пауза — после обрыва второго стрима.
    assert fast_sleep == [2.0]
    assert factory.teardowns >= 2


async def test_not_found_instruments_are_logged(
    collectors: dict[str, DataCollector],
    fast_sleep: list[float],
    caplog: pytest.LogCaptureFixture,
) -> None:
    """SPEC-0009: инструмент без FIGI не входит в подписку, о чём есть предупреждение."""
    hub_ref: list[StreamHub] = []

    async def factory(instruments: Sequence[Instrument], resolve: bool) -> HubConnection:
        await hub_ref[0].shutdown()
        return HubConnection(
            stream=ListStream([]), routes={"FIGI_SBER": "SBER"}, not_found=["GAZP"]
        )

    hub = StreamHub(list(collectors.values()), factory)
    hub_ref.append(hub)
    with caplog.at_level(logging.WARNING):
        await _run(hub)
    assert "Инструмент GAZP не найден у провайдера" in caplog.text


def test_subscription_status_and_delays() -> None:
    assert subscription_ok(1)
    assert subscription_ok("SUBSCRIPTION_STATUS_SUCCESS")
    assert not subscription_ok(6)
    assert not subscription_ok("SUBSCRIPTION_STATUS_LIMIT_IS_EXCEEDED")
    delays = [0.0]
    for _ in range(7):
        delays.append(_next_delay(delays[-1]))
    assert delays[1:] == [2.0, 4.0, 8.0, 16.0, 32.0, 60.0, 60.0]
