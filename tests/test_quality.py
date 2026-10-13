"""SPEC-0004: детекторы деградации данных."""

from __future__ import annotations

import logging
from collections.abc import Sequence
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import Any

import pytest

from cluster_source import collector as collector_mod
from cluster_source.collector import DataCollector
from cluster_source.config import AppConfig, Instrument, InstrumentType
from cluster_source.database import MarketDataRepository
from cluster_source.quality import (
    REASON_BACKLOG,
    REASON_CONTRACT_ROLL,
    REASON_DEALER_LEAK,
    REASON_LATENCY,
    REASON_STREAM_GAP,
    REASON_STREAM_SILENCE,
    REASON_SUBSCRIPTION_FAILED,
    REASON_UNKNOWN_DIRECTION,
    REASON_WRITE_FAILURE,
    STATE_DEGRADED,
    STATE_OK,
    InstrumentQuality,
    minutes_between,
)
from cluster_source.reader import get_data_state, get_degraded_minutes
from cluster_source.stream_hub import HubConnection, StreamHub

UTC = UTC
SBER = Instrument(id="SBER", type=InstrumentType.SHARE, tick_size=0.01, lot_size=1)
GAZP = Instrument(id="GAZP", type=InstrumentType.SHARE, tick_size=0.01, lot_size=1)
SI = Instrument(id="Si", type=InstrumentType.CONTINUOUS_FUTURES, tick_size=1, lot_size=1)
T0 = datetime(2026, 10, 13, 8, 0, 0, tzinfo=UTC)
NORMAL = 5
BREAK = 4


class Clock:
    def __init__(self, moment: datetime) -> None:
        self.moment = moment

    def __call__(self) -> datetime:
        return self.moment

    def advance(self, seconds: float) -> datetime:
        self.moment += timedelta(seconds=seconds)
        return self.moment


def trade(figi: str = "F_SBER", at: datetime = T0, direction: int = 1, source: int = 1) -> Any:
    return SimpleNamespace(
        figi=figi,
        instrument_uid="",
        price=100.0,
        quantity=1,
        direction=direction,
        time=at,
        trade_source=source,
    )


def marks(c: DataCollector, reason: str) -> set[str]:
    minutes, _ = c.quality.drain()
    return {m for m, r in minutes if r == reason}


@pytest.fixture
def clock() -> Clock:
    return Clock(T0)


def make_collector(config: AppConfig, instr: Instrument, clock: Clock) -> DataCollector:
    return DataCollector(config, instr, MarketDataRepository(config.storage_root, instr), now=clock)


@pytest.fixture
def hub_env(share_config: AppConfig, clock: Clock) -> tuple[StreamHub, dict[str, DataCollector]]:
    cols = {i.storage_name: make_collector(share_config, i, clock) for i in (SBER, GAZP)}

    async def factory(instruments: Sequence[Instrument], resolve: bool) -> HubConnection:
        raise AssertionError("в тестах сторожа соединение не открывается")

    return StreamHub(list(cols.values()), factory, now=clock), cols


ROUTES = {"F_SBER": "SBER", "F_GAZP": "GAZP"}


def open_conn(hub: StreamHub, routes: dict[str, str] | None = None) -> None:
    hub._on_opened(HubConnection(stream=None, routes=routes or ROUTES))


# --------------------------------------------------------------------------- #
# Модель условий и отметок
# --------------------------------------------------------------------------- #
def test_condition_lifecycle_events(caplog: pytest.LogCaptureFixture, clock: Clock) -> None:
    """SPEC-0004 A-3 (G-2): переходы OK ↔ DEGRADED — события и маркеры в логе."""
    q = InstrumentQuality("SBER")
    with caplog.at_level(logging.INFO):
        q.begin(REASON_LATENCY, T0, T0)
        q.begin(REASON_LATENCY, T0, T0)  # повтор не создаёт события
        q.tick(T0 + timedelta(minutes=2))
        q.end(REASON_LATENCY, T0 + timedelta(minutes=3))
    minutes, events = q.drain()
    assert {m for m, _ in minutes} == {(T0 + timedelta(minutes=k)).isoformat() for k in range(4)}
    assert [(state, reasons) for _, state, reasons in events] == [
        (STATE_DEGRADED, "latency"),
        (STATE_OK, ""),
    ]
    assert "DATA_DEGRADED SBER: latency" in caplog.text
    assert "DATA_RESTORED SBER" in caplog.text
    assert q.state == STATE_OK


def test_one_off_mark_keeps_state() -> None:
    q = InstrumentQuality("SBER")
    q.mark(REASON_DEALER_LEAK, T0 + timedelta(seconds=30))
    minutes, events = q.drain()
    assert minutes == [(T0.isoformat(), REASON_DEALER_LEAK)]
    assert events == [] and q.state == STATE_OK


def test_minutes_between_inclusive() -> None:
    assert len(minutes_between(T0 + timedelta(seconds=59), T0 + timedelta(seconds=61))) == 2


# --------------------------------------------------------------------------- #
# Детекторы хаба
# --------------------------------------------------------------------------- #
async def test_stream_gap_from_startup_until_subscription_confirmed(
    hub_env: tuple[StreamHub, dict[str, DataCollector]], clock: Clock
) -> None:
    """SPEC-0004 A-1 stream_gap: от начала суток до подтверждения подписки."""
    hub, cols = hub_env
    await hub._begin_startup_gap()
    assert cols["SBER"].quality.since(REASON_STREAM_GAP) == T0.replace(hour=0)

    clock.advance(30)
    open_conn(hub)
    assert cols["SBER"].quality.is_active(REASON_STREAM_GAP)  # открытие ещё не подтверждение
    response = SimpleNamespace(
        trade_subscriptions=[SimpleNamespace(figi="F_SBER", subscription_status=1)]
    )
    hub._check_subscriptions(response, ROUTES)
    assert not cols["SBER"].quality.is_active(REASON_STREAM_GAP)
    await hub._route(trade("F_GAZP"), ROUTES)  # первая сделка тоже подтверждает
    assert not cols["GAZP"].quality.is_active(REASON_STREAM_GAP)
    assert len(marks(cols["SBER"], REASON_STREAM_GAP)) == 8 * 60 + 1


async def test_stream_gap_on_connection_loss(
    hub_env: tuple[StreamHub, dict[str, DataCollector]], clock: Clock
) -> None:
    """SPEC-0004 A-1 stream_gap: обрыв — с последнего кадра."""
    hub, cols = hub_env
    open_conn(hub)
    await hub._route(trade("F_SBER"), ROUTES)
    hub._last_frame_at = clock.advance(10)
    clock.advance(70)
    hub._on_lost()
    assert cols["SBER"].quality.since(REASON_STREAM_GAP) == T0 + timedelta(seconds=10)
    assert cols["GAZP"].quality.state == STATE_DEGRADED


def test_subscription_failed(
    hub_env: tuple[StreamHub, dict[str, DataCollector]], caplog: pytest.LogCaptureFixture
) -> None:
    """SPEC-0004 A-1 subscription_failed; SPEC-0009 G-3."""
    hub, cols = hub_env
    open_conn(hub)
    response = SimpleNamespace(
        trade_subscriptions=[SimpleNamespace(figi="F_GAZP", subscription_status=6)]
    )
    with caplog.at_level(logging.ERROR):
        hub._check_subscriptions(response, ROUTES)
    assert cols["GAZP"].quality.is_active(REASON_SUBSCRIPTION_FAILED)
    assert "STREAM_SUBSCRIPTION_FAILED GAZP" in caplog.text
    open_conn(hub)  # новое открытие снимает условие до нового ответа
    assert not cols["GAZP"].quality.is_active(REASON_SUBSCRIPTION_FAILED)


async def test_silence_during_normal_trading(
    hub_env: tuple[StreamHub, dict[str, DataCollector]], clock: Clock
) -> None:
    """SPEC-0004 A-1 stream_silence: торги идут, сделок нет дольше порога; сделка снимает."""
    hub, cols = hub_env
    open_conn(hub)
    hub._on_trading_status(SimpleNamespace(figi="F_SBER", trading_status=NORMAL, time=T0), ROUTES)
    # первая сделка подтверждает подписку (начало прослушивания)
    for _ in range(30):  # сделки раз в 0,2 с → порог 60 с (минимум)
        clock.advance(0.2)
        await hub._route(trade("F_SBER", at=clock()), ROUTES)
    assert cols["SBER"].silence_threshold() == 60.0
    last = clock()
    hub.check(clock.advance(59))
    assert not cols["SBER"].quality.is_active(REASON_STREAM_SILENCE)
    hub.check(clock.advance(2))
    assert cols["SBER"].quality.since(REASON_STREAM_SILENCE) == last
    await hub._route(trade("F_SBER", at=clock()), ROUTES)
    assert not cols["SBER"].quality.is_active(REASON_STREAM_SILENCE)


async def test_silence_counted_from_listening_start(
    hub_env: tuple[StreamHub, dict[str, DataCollector]], clock: Clock
) -> None:
    """SPEC-0004: тишина отсчитывается не раньше подтверждения подписки (живой случай 2026-10-13).

    Статус NORMAL_TRADING с 07:00 МСК, подписались в 08:00 — малоликвидная бумага без
    сделок в первую минуту не должна сразу считаться «тихой».
    """
    hub, cols = hub_env
    open_conn(hub)
    hub._on_trading_status(
        SimpleNamespace(figi="F_GAZP", trading_status=NORMAL, time=T0 - timedelta(hours=1)),
        ROUTES,
    )
    hub.check(clock.advance(60))
    assert not cols["GAZP"].quality.is_active(REASON_STREAM_SILENCE)  # ещё не слушаем
    hub._check_subscriptions(
        SimpleNamespace(
            trade_subscriptions=[SimpleNamespace(figi="F_GAZP", subscription_status=1)]
        ),
        ROUTES,
    )
    listening = clock()
    hub.check(clock.advance(899))
    assert not cols["GAZP"].quality.is_active(REASON_STREAM_SILENCE)
    hub.check(clock.advance(2))
    assert cols["GAZP"].quality.since(REASON_STREAM_SILENCE) == listening


async def test_silence_ignored_outside_normal_trading(
    hub_env: tuple[StreamHub, dict[str, DataCollector]], clock: Clock
) -> None:
    """SPEC-0004 A-2 (G-4): перерыв в торгах — тишина не деградация."""
    hub, cols = hub_env
    open_conn(hub)
    hub._on_trading_status(SimpleNamespace(figi="F_SBER", trading_status=BREAK, time=T0), ROUTES)
    hub.check(clock.advance(3600))
    assert not cols["SBER"].quality.is_active(REASON_STREAM_SILENCE)


def test_silence_threshold_is_adaptive(share_config: AppConfig, clock: Clock) -> None:
    c = make_collector(share_config, SBER, clock)
    assert c.silence_threshold() == collector_mod.SILENCE_MAX_SEC  # мало данных — максимум
    c._intervals.extend([10.0] * 50)
    assert c.silence_threshold() == 200.0  # 20 × p95


async def test_latency_with_hysteresis(
    hub_env: tuple[StreamHub, dict[str, DataCollector]], clock: Clock
) -> None:
    """SPEC-0004 A-1 latency: медиана отставания > 5 с — у всех; < 3 с — снятие."""
    hub, cols = hub_env
    open_conn(hub)
    for _ in range(5):
        await hub._route(trade("F_SBER", at=clock() - timedelta(seconds=8)), ROUTES)
    hub.check(clock())
    assert all(c.quality.is_active(REASON_LATENCY) for c in cols.values())
    clock.advance(31)
    for _ in range(5):
        await hub._route(trade("F_SBER", at=clock() - timedelta(seconds=1)), ROUTES)
    hub.check(clock())
    assert not any(c.quality.is_active(REASON_LATENCY) for c in cols.values())


def test_dead_stream_requests_reconnect(
    hub_env: tuple[StreamHub, dict[str, DataCollector]],
    clock: Clock,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """SPEC-0004: при идущих торгах нет кадров 180 с — переподключение."""
    hub, cols = hub_env
    open_conn(hub)
    hub._on_trading_status(SimpleNamespace(figi="F_SBER", trading_status=NORMAL, time=T0), ROUTES)
    with caplog.at_level(logging.ERROR):
        hub.check(clock.advance(181))
    assert hub._reconnect_requested
    assert "Стрим сделок мёртв" in caplog.text


def test_contract_roll_marked(share_config: AppConfig, clock: Clock) -> None:
    """SPEC-0004 A-1 contract_roll: сменился FIGI непрерывного фьючерса."""
    si = make_collector(share_config, SI, clock)

    async def factory(instruments: Sequence[Instrument], resolve: bool) -> HubConnection:
        raise AssertionError

    hub = StreamHub([si], factory, now=clock)
    open_conn(hub, {"F_SIU6": "Si_CONTINUOUS"})
    open_conn(hub, {"F_SIU6": "Si_CONTINUOUS"})
    assert not marks(si, REASON_CONTRACT_ROLL)
    open_conn(hub, {"F_SIZ6": "Si_CONTINUOUS"})
    assert marks(si, REASON_CONTRACT_ROLL) == {T0.isoformat()}


# --------------------------------------------------------------------------- #
# Детекторы коллектора
# --------------------------------------------------------------------------- #
async def test_dealer_leak_marked(share_config: AppConfig, clock: Clock) -> None:
    c = make_collector(share_config, SBER, clock)
    await c.deliver(trade(source=2))
    assert marks(c, REASON_DEALER_LEAK) == {T0.isoformat()}


async def test_backlog(
    share_config: AppConfig, clock: Clock, monkeypatch: pytest.MonkeyPatch
) -> None:
    """SPEC-0004 A-1 backlog: очередь выше порога — условие; разбор очереди — снятие."""
    monkeypatch.setattr(collector_mod, "BACKLOG_HIGH", 3)
    monkeypatch.setattr(collector_mod, "BACKLOG_LOW", 1)
    c = make_collector(share_config, SBER, clock)
    for _ in range(5):
        await c.deliver(trade())
    assert c.quality.is_active(REASON_BACKLOG)
    while not c._queue.empty():
        c._queue.get_nowait()
    await c._flush([])
    assert not c.quality.is_active(REASON_BACKLOG)


async def test_write_failure(share_config: AppConfig, clock: Clock) -> None:
    """SPEC-0004 A-1 write_failure: минуты потерянного пакета; успех снимает условие."""
    c = make_collector(share_config, SBER, clock)
    tick = collector_mod._to_dto(trade(at=T0 - timedelta(minutes=2)), SBER)

    def boom(rows: Any) -> None:
        raise OSError("disk full")

    original = c._repo.insert_ticks
    c._repo.insert_ticks = boom  # type: ignore[method-assign]
    await c._flush([tick])
    assert c.quality.is_active(REASON_WRITE_FAILURE)
    c._repo.insert_ticks = original  # type: ignore[method-assign]
    await c._flush([tick])
    assert not c.quality.is_active(REASON_WRITE_FAILURE)
    minutes = c._repo.read_degraded_minutes(T0 - timedelta(minutes=5), T0 + timedelta(minutes=1))
    assert (T0 - timedelta(minutes=2)) in set(minutes["minute"])


async def test_unknown_direction_minute(share_config: AppConfig, clock: Clock) -> None:
    """SPEC-0004 A-1 unknown_direction: при торгах в минуте > 50% сделок без стороны."""
    c = make_collector(share_config, SBER, clock)
    c.set_trading_status(NORMAL, T0)
    for i in range(25):
        await c.deliver(trade(at=T0 + timedelta(seconds=i), direction=0 if i < 20 else 1))
    await c.deliver(trade(at=T0 + timedelta(minutes=1)))
    assert marks(c, REASON_UNKNOWN_DIRECTION) == {T0.isoformat()}

    c.set_trading_status(9, T0)  # аукцион открытия — не считается
    for i in range(25):
        await c.deliver(trade(at=T0 + timedelta(minutes=2, seconds=i), direction=0))
    c.close_minutes(T0 + timedelta(minutes=4))
    assert not marks(c, REASON_UNKNOWN_DIRECTION)


# --------------------------------------------------------------------------- #
# Сохранение и чтение
# --------------------------------------------------------------------------- #
async def test_quality_persisted_by_consumer(share_config: AppConfig, clock: Clock) -> None:
    """SPEC-0004 A-4 (G-3): продюсер не пишет в БД; ошибка записи — отметки ждут следующей."""
    c = make_collector(share_config, SBER, clock)
    c.quality.begin(REASON_LATENCY, T0, T0)
    assert c._repo.quality_state() is None  # пока не было записи консьюмера

    original = c._repo.save_quality

    def boom(*args: Any) -> None:
        raise OSError("locked")

    c._repo.save_quality = boom  # type: ignore[method-assign]
    await c._flush([])
    c._repo.save_quality = original  # type: ignore[method-assign]
    await c._flush([])
    assert c._repo.quality_state() == {
        "since": T0.isoformat(),
        "state": STATE_DEGRADED,
        "reasons": ["latency"],
    }


async def test_reader_quality_api(share_config: AppConfig, clock: Clock) -> None:
    """SPEC-0004 A-5: робот читает минуты DEGRADED и текущее состояние."""
    c = make_collector(share_config, SBER, clock)
    assert get_data_state(c._repo)["state"] == "UNKNOWN"
    c.quality.begin(REASON_STREAM_GAP, T0, T0 + timedelta(minutes=1))
    c.quality.mark(REASON_DEALER_LEAK, T0 + timedelta(minutes=1, seconds=5))
    c.quality.end(REASON_STREAM_GAP, T0 + timedelta(minutes=1))
    await c._flush([])

    df = get_degraded_minutes(c._repo, T0, T0 + timedelta(minutes=5))
    assert list(df["minute"]) == [T0, T0 + timedelta(minutes=1)]
    assert set(df.iloc[1]["reasons"].split(",")) == {"stream_gap", "dealer_leak"}
    assert get_data_state(c._repo)["state"] == STATE_OK
