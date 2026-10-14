"""SPEC-0008: сигналы по объёмам — детекторы, отсев, проверка."""

from __future__ import annotations

import json
import logging
from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from typing import Any

import pandas as pd
import pytest
from conftest import make_config

from cluster_source.collector import DataCollector, Tick
from cluster_source.config import AppConfig, Instrument, InstrumentType, SignalsConfig
from cluster_source.database import MarketDataRepository, SignalRepository
from cluster_source.quality import REASON_STREAM_SILENCE, REASON_UNKNOWN_DIRECTION
from cluster_source.signals import engine as engine_mod
from cluster_source.signals.baseline import Norms, compute_norms
from cluster_source.signals.detectors import (
    LARGE_AGGRESSOR,
    VOLUME_SPIKE,
    Candidate,
    LargeAggressorDetector,
    VolumeSpikeDetector,
    recheck_large_aggressor,
)
from cluster_source.signals.engine import SignalEngine

UTC = UTC
SBER = Instrument(id="SBER", type=InstrumentType.SHARE, tick_size=0.01, lot_size=10)
SI = Instrument(id="Si", type=InstrumentType.CONTINUOUS_FUTURES, tick_size=1, lot_size=1)
#: Вторник 13.10, 10:31 МСК — основная сессия.
T0 = datetime(2026, 10, 13, 7, 31, tzinfo=UTC)
NORMS = Norms(
    days_final=20,
    days_archive=0,
    aggressor_threshold={"main": 1_000_000.0},
    minute_median={"weekday 10:31": 100.0, "weekday 10:32": 100.0},
    minute_volume_pct={"main": 200.0},
)
FUT_NORMS = replace(NORMS, days_final=15, days_archive=5, aggressor_threshold={"main": 500.0})


def tick(moment: datetime, volume: int, direction: str = "BUY", price: float = 300.0,
         name: str = "SBER") -> Tick:  # fmt: skip
    return Tick(name, moment, price, volume, direction, f"s{moment.timestamp()}")


@pytest.fixture
def cfg(tmp_path: Any) -> AppConfig:
    config = make_config(tmp_path, [SBER, SI])
    return replace(config, signals=SignalsConfig(enabled=True))


# --------------------------------------------------------------------------- #
# Детекторы
# --------------------------------------------------------------------------- #
def test_large_aggressor_parent_order(cfg: AppConfig) -> None:
    """SPEC-0008 A-1: склейка по времени и стороне, порог по нормам, аукционы не участвуют."""
    det = LargeAggressorDetector(SBER, NORMS, cfg.signals)
    t = T0 + timedelta(seconds=5, microseconds=123456)
    assert det.on_tick(tick(t, 300, price=300.0), t) == []  # 900 000 ₽ — ниже порога
    assert det.on_tick(tick(t, 100, price=301.0), t) == []  # та же заявка: 1 201 000 ₽
    nxt = t + timedelta(milliseconds=1)
    out = det.on_tick(tick(nxt, 1, "SELL"), nxt)  # новая заявка закрывает прежнюю
    assert len(out) == 1
    cand = out[0]
    assert (cand.event_type, cand.direction, cand.cls, cand.timeframe) == (
        LARGE_AGGRESSOR, "long", "fast", "tick",
    )  # fmt: skip
    assert (cand.window_from, cand.window_to, cand.moment) == (T0, T0 + timedelta(minutes=1), t)
    p = cand.payload
    assert (p["quantity"], p["trades"], p["size"], p["size_unit"], p["regime"]) == (
        400, 2, 1_201_000.0, "rub", "main",
    )  # fmt: skip
    assert det.flush(nxt + timedelta(seconds=3)) == []  # заявка на 3 000 ₽ — не сигнал

    huge = t + timedelta(seconds=1)
    assert det.on_tick(tick(huge, 100_000, "UNKNOWN"), huge) == []  # аукцион — не агрессия
    assert det.flush(huge + timedelta(seconds=5)) == []

    fut = LargeAggressorDetector(SI, FUT_NORMS, cfg.signals)
    fut.on_tick(tick(t, 600, "SELL", 85000.0, "Si_CONTINUOUS"), t)
    out = fut.flush(t + timedelta(seconds=3))  # заявка закрыта по простою
    assert [(c.direction, c.payload["size"], c.payload["size_unit"]) for c in out] == [
        ("short", 600.0, "contracts")
    ]


def test_volume_spike_same_minute_of_day(cfg: AppConfig) -> None:
    """SPEC-0008 A-2: объём / медиана той же минуты дня ≥ k и ≥ перцентиля режима."""
    det = VolumeSpikeDetector(SBER, NORMS, cfg.signals)
    det.on_tick(tick(T0 + timedelta(seconds=1), 500, "BUY"), T0)
    det.on_tick(tick(T0 + timedelta(seconds=2), 100, "SELL"), T0)
    t1 = T0 + timedelta(minutes=1, seconds=1)
    out = det.on_tick(tick(t1, 250, "BUY"), t1)  # новая минута закрывает прежнюю
    assert len(out) == 1
    cand = out[0]
    assert (cand.event_type, cand.direction, cand.timeframe, cand.bar_closed) == (
        VOLUME_SPIKE, "long", "1m", True,
    )  # fmt: skip
    assert (cand.payload["volume"], cand.payload["ratio"], cand.payload["delta"]) == (600, 6.0, 400)
    det.on_tick(tick(t1, 250, "SELL"), t1)  # 500 / 100 = 5 — порог, дельта 0
    out = det.flush(t1 + timedelta(minutes=1, seconds=5))
    assert [(c.direction, c.payload["ratio"]) for c in out] == [("neutral", 5.0)]

    det.reset()
    det.on_tick(tick(T0, 400, "BUY"), T0)  # 4 × медиана — ниже порога
    assert det.flush(T0 + timedelta(minutes=2)) == []
    det.on_tick(tick(T0 + timedelta(minutes=5), 900, "BUY"), T0)  # 10:36 — нормы нет
    assert det.flush(T0 + timedelta(minutes=7)) == []


# --------------------------------------------------------------------------- #
# Движок: отсев, разветвление, проверка
# --------------------------------------------------------------------------- #
def make_engine(cfg: AppConfig, now: list[datetime]) -> tuple[SignalEngine, Any, Any]:
    repos = {i.storage_name: MarketDataRepository(cfg.storage_root, i) for i in cfg.instruments}
    collectors = {
        i.storage_name: DataCollector(cfg, i, repos[i.storage_name]) for i in cfg.instruments
    }
    signal_repo = SignalRepository(cfg.storage_root)
    engine = SignalEngine(
        cfg, repos, collectors, signal_repo, lambda n: "SiZ6" if n == "Si_CONTINUOUS" else None,
        now=lambda: now[0],
    )  # fmt: skip
    engine._norms = {"SBER": NORMS, "Si_CONTINUOUS": FUT_NORMS}
    return engine, repos, signal_repo


def candidate(moment: datetime = T0 + timedelta(seconds=5)) -> Candidate:
    start = moment.replace(second=0, microsecond=0)
    return Candidate(
        event_type=LARGE_AGGRESSOR, cls="fast", direction="long", timeframe="tick",
        window_from=start, window_to=start + timedelta(minutes=1), bar_closed=False,
        moment=moment, key_extra=moment.isoformat(),
        payload={"trade_time": moment.isoformat().replace("+00:00", "Z"), "side": "BUY",
                 "price_min": 300.0, "price_max": 300.0, "quantity": 1000, "trades": 3,
                 "size": 3_000_000.0, "size_unit": "rub", "threshold": 1_000_000.0,
                 "percentile": 99.9, "regime": "main"},
    )  # fmt: skip


def statuses(signal_repo: SignalRepository) -> list[tuple[str, str | None]]:
    df = signal_repo.read_signals("2000-01-01", "2100-01-01")
    return [
        (status, None if pd.isna(reason) else reason)
        for status, reason in zip(df["status"], df["reason"], strict=True)
    ]


async def test_signal_suppressed_on_degraded(cfg: AppConfig) -> None:
    """SPEC-0008 A-3 (G-3): DEGRADED сейчас, отметка в окне, пауза — сигнал не публикуется."""
    now = [T0 + timedelta(seconds=6)]
    engine, _repos, signal_repo = make_engine(cfg, now)
    quality = engine._collectors["SBER"].quality

    quality.begin(REASON_STREAM_SILENCE, T0 - timedelta(minutes=5), T0)
    await engine.emit("SBER", candidate())
    quality.end(REASON_STREAM_SILENCE, T0 - timedelta(minutes=3))
    await engine.emit("SBER", candidate(T0 + timedelta(minutes=10)))  # чисто: опубликован
    quality.mark(REASON_UNKNOWN_DIRECTION, T0 + timedelta(minutes=20))
    await engine.emit("SBER", candidate(T0 + timedelta(minutes=20, seconds=1)))
    engine._pause("SBER", T0 + timedelta(minutes=30))
    await engine.emit("SBER", candidate(T0 + timedelta(minutes=31, seconds=1)))

    assert statuses(signal_repo) == [
        ("SUPPRESSED", "degraded"),
        ("PENDING", None),
        ("SUPPRESSED", "degraded_minute"),
        ("SUPPRESSED", "signals_paused"),
    ]
    assert signal_repo.outbox_backlog() == 1  # отсеянные в outbox не попадают
    assert engine.suppressed == {"degraded": 1, "degraded_minute": 1, "signals_paused": 1}


def put_verdict(
    repo: MarketDataRepository, minute: datetime, status: str, detail: str = ""
) -> None:
    nxt = minute + timedelta(minutes=1)
    repo.apply_minute_verdict(
        minute.isoformat(), (status, 3, 3, 0, detail), [], [], [], nxt.isoformat()
    )


@pytest.mark.parametrize(
    ("verdict", "detail", "expected"),
    [
        ("VERIFIED", "", ("CONFIRMED", "VERIFIED", None)),
        ("GAP", "extra_trades: 1 с, первая 07:31:05", ("REVOKED", None, "extra_trades")),
        ("GAP", "iss_empty", ("REVOKED", None, "gap")),
        (None, "", ("REVOKED", None, "verification_unavailable")),
    ],
)
async def test_verification_outcomes(
    cfg: AppConfig, verdict: str | None, detail: str, expected: tuple[str, str | None, str | None]
) -> None:
    """SPEC-0008 A-4 (G-4): итог по вердиктам минут окна; ровно один итог к сроку."""
    now = [T0 + timedelta(seconds=6)]
    engine, repos, signal_repo = make_engine(cfg, now)
    await engine.emit("SBER", candidate())
    now[0] = T0 + timedelta(minutes=10)
    await engine.verify_once()
    assert statuses(signal_repo) == [("PENDING", None)]  # вердиктов ещё нет, срок не вышел

    if verdict is not None:
        put_verdict(repos["SBER"], T0, verdict, detail)
    else:
        now[0] = T0 + timedelta(minutes=21)  # срок: конец окна + 20 минут
    await engine.verify_once()
    await engine.verify_once()  # повтор не даёт второго итога
    df = signal_repo.read_signals("2000-01-01", "2100-01-01")
    row = df.iloc[0]
    assert (row["status"], row["outcome"], row["reason"]) == expected
    assert signal_repo.outbox_backlog() == 2  # сигнал и один итог
    events = [json.loads(e[2]) for e in signal_repo.due_events("2100-01-01")]
    assert [e["category"] for e in events] == ["signal", "signal_result"]
    assert events[1]["signal_id"] == events[0]["signal_id"]


@pytest.mark.parametrize("iss_volume", [1000, 10])
async def test_large_aggressor_recheck_on_repaired(cfg: AppConfig, iss_volume: int) -> None:
    """SPEC-0008 A-5: REPAIRED — секунда заявки пересчитывается по сделкам ISS."""
    now = [T0 + timedelta(seconds=6)]
    engine, repos, signal_repo = make_engine(cfg, now)
    await engine.emit("SBER", candidate())
    second = T0 + timedelta(seconds=5)
    repos["SBER"].insert_ticks(
        [{"ticker": "SBER", "timestamp": second, "price": 300.0, "volume": iss_volume,
          "direction": "BUY", "trade_id": "x1", "origin": "iss"}]
    )  # fmt: skip
    put_verdict(repos["SBER"], T0, "REPAIRED", "дозалито 1 сделок")
    now[0] = T0 + timedelta(minutes=18)
    await engine.verify_once()
    row = signal_repo.read_signals("2000-01-01", "2100-01-01").iloc[0]
    if iss_volume == 1000:  # 1000 × 300 × 10 = 3 млн ₽ ≥ порога 1 млн
        assert (row["status"], row["outcome"]) == ("CONFIRMED", "REPAIRED")
    else:
        assert (row["status"], row["reason"]) == ("REVOKED", "repaired_changed_result")


def test_recheck_large_aggressor_untouched_second() -> None:
    """SPEC-0008 A-5: секунда заявки не заменялась — сигнал в силе."""
    payload = {"side": "BUY", "threshold": 1_000_000.0}
    stream = pd.DataFrame(
        {"price": [300.0], "volume": [1], "direction": ["BUY"], "origin": ["stream"]}
    )
    assert recheck_large_aggressor(payload, stream, SBER)
    assert recheck_large_aggressor(payload, stream.iloc[0:0], SBER)


async def test_fanout_overflow_pauses_signals(
    cfg: AppConfig, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """SPEC-0008 A-10 (G-2): разветвление не блокирует сбор; переполнение — пауза сигналов."""
    monkeypatch.setattr(engine_mod, "FANOUT_CAPACITY", 2)
    engine, _repos, _signal_repo = make_engine(cfg, [T0])
    collector = engine._collectors["SBER"]
    collector.tick_listener = engine.feed
    trade = type("Trade", (), {"trade_source": "TRADE_SOURCE_EXCHANGE", "time": T0,
                               "price": 300.0, "quantity": 1, "direction": 1})  # fmt: skip
    with caplog.at_level(logging.WARNING):
        for _ in range(3):
            await collector.deliver(trade())  # не ждёт детекторов
    assert engine._queue.qsize() == 2
    assert engine._paused["SBER"] == (T0, T0 + timedelta(minutes=2))
    assert "SIGNALS_PAUSED SBER" in caplog.text
    assert engine.gate("SBER", candidate()) == "signals_paused"


# --------------------------------------------------------------------------- #
# Нормы (G-6)
# --------------------------------------------------------------------------- #
async def test_detector_off_without_baseline(
    cfg: AppConfig, caplog: pytest.LogCaptureFixture
) -> None:
    """SPEC-0008 A-9 (G-6): нормы только по get_baseline_days; меньше 5 дней — выключен."""
    repo = MarketDataRepository(cfg.storage_root, SBER)
    days = [date(2026, 10, d) for d in (5, 6, 7, 8, 9)]  # пн–пт
    for k, day in enumerate(days):
        base = datetime(day.year, day.month, day.day, 8, tzinfo=UTC)  # 11:00 МСК
        repo.insert_ticks(
            [{"ticker": "SBER", "timestamp": base + timedelta(seconds=i), "price": 300.0,
              "volume": 1 + i % 7, "direction": "BUY" if i % 2 else "SELL", "trade_id": f"s{i}"}
             for i in range(60)]
        )  # fmt: skip
        if k < 4:
            repo.set_day_verdict(day.isoformat(), "FINAL", (1, 1.0), (1, 1.0), "iss_totals", "")
    assert compute_norms(repo, SBER, cfg.signals) is None  # 4 дня

    engine, _repos, _signal_repo = make_engine(cfg, [T0])
    engine._repos = {"SBER": repo, "Si_CONTINUOUS": engine._repos["Si_CONTINUOUS"]}
    with caplog.at_level(logging.WARNING):
        await engine.refresh_norms()
    assert "SIGNAL_DETECTOR_OFF SBER" in caplog.text
    assert "SBER" not in engine._norms and engine._detectors["SBER"] == []

    repo.set_day_verdict(days[4].isoformat(), "ARCHIVE_CONSISTENT", (1, 1.0), (1, 1.0), "x", "")
    loose = replace(cfg.signals, large_aggressor_percentile=50.0)
    norms = compute_norms(repo, SBER, loose)
    assert norms is not None
    assert (norms.baseline_days, norms.quality) == ({"final": 4, "archive": 1}, "mixed")
    assert set(norms.aggressor_threshold) == {"main"}  # 300 заявок основной сессии
    assert norms.minute_median["weekday 11:00"] > 0
