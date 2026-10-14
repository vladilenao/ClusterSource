"""SPEC-0013: активный контракт по последнему дню торгов; пропуски торговых дней."""

from __future__ import annotations

import logging
from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from types import SimpleNamespace
from typing import Any

import pytest
from conftest import make_config

from cluster_source import main as main_mod
from cluster_source.autoload import (
    REFRESH_CALENDAR_GAP,
    _fetch_futures_rest,
    futures_from_sdk,
    refresh_days,
)
from cluster_source.config import (
    AppConfig,
    FuturesMeta,
    Instrument,
    InstrumentType,
    nearest_contract,
)
from cluster_source.database import IssTapeRepository, MarketDataRepository
from cluster_source.schedule import find_calendar_gaps, mark_calendar_gaps

UTC = UTC
SI = Instrument(id="Si", type=InstrumentType.CONTINUOUS_FUTURES, tick_size=1, lot_size=1)
GD = Instrument(id="GD", type=InstrumentType.CONTINUOUS_FUTURES, tick_size=0.1, lot_size=1)
SBER = Instrument(id="SBER", type=InstrumentType.SHARE, tick_size=0.01, lot_size=10)
FUTURES = [
    FuturesMeta("SiU6", "SPBFUT", date(2026, 9, 18), "F1", last_trade=date(2026, 9, 17)),
    FuturesMeta("SiZ6", "SPBFUT", date(2026, 12, 18), "F2", last_trade=date(2026, 12, 17)),
    FuturesMeta("GDU6", "SPBFUT", date(2026, 9, 21), "F3", last_trade=date(2026, 9, 18)),
    FuturesMeta("GDZ6", "SPBFUT", date(2026, 12, 21), "F4", last_trade=date(2026, 12, 18)),
]


def test_active_contract_by_last_trade() -> None:
    """SPEC-0013 A-1 (G-1): день после последнего торга — следующий контракт."""

    def pick(inst: Instrument, day: str) -> str | None:
        meta = nearest_contract(inst, FUTURES, date.fromisoformat(day))
        return None if meta is None else meta.ticker

    assert pick(SI, "2026-09-17") == "SiU6"  # последний день торгов SiU6
    assert pick(SI, "2026-09-18") == "SiZ6"  # день исполнения SiU6 — уже SiZ6
    assert pick(GD, "2026-09-18") == "GDU6"
    assert [pick(GD, d) for d in ("2026-09-19", "2026-09-20", "2026-09-21")] == ["GDZ6"] * 3
    old = [FuturesMeta("SiU6", "SPBFUT", date(2026, 9, 18))]  # без last_trade — по исполнению
    assert nearest_contract(SI, old, date(2026, 9, 18)).ticker == "SiU6"  # type: ignore[union-attr]


async def test_futures_meta_reads_last_trade() -> None:
    """SPEC-0013 A-2: справочник SDK и REST читает last_trade_date; пустое значение — None."""
    sdk = futures_from_sdk(
        [
            SimpleNamespace(ticker="SiU6", class_code="SPBFUT", figi="F1",
                            expiration_date=datetime(2026, 9, 18, tzinfo=UTC),
                            last_trade_date=datetime(2026, 9, 17, tzinfo=UTC)),
            SimpleNamespace(ticker="SiZ6", class_code="SPBFUT", figi="F2",
                            expiration_date=datetime(2026, 12, 18, tzinfo=UTC),
                            last_trade_date=datetime(1970, 1, 1, tzinfo=UTC)),
        ]
    )  # fmt: skip
    assert sdk["SiU6"].last_trade == date(2026, 9, 17)
    assert (sdk["SiZ6"].last_trade, sdk["SiZ6"].active_until) == (None, date(2026, 12, 18))

    class Resp:
        async def __aenter__(self) -> Resp:
            return self

        async def __aexit__(self, *_: Any) -> None:
            return None

        def raise_for_status(self) -> None:
            return None

        async def json(self) -> dict[str, Any]:
            return {"instruments": [{"ticker": "BRU6", "classCode": "SPBFUT", "figi": "F",
                                     "expirationDate": "2026-09-01T00:00:00Z",
                                     "lastTradeDate": "2026-08-31T00:00:00Z"}]}  # fmt: skip

    session = SimpleNamespace(post=lambda *a, **k: Resp())
    rest = await _fetch_futures_rest(session, "token")  # type: ignore[arg-type]
    assert rest["BRU6"].last_trade == date(2026, 8, 31)


# --------------------------------------------------------------------------- #
@pytest.fixture
def cfg(tmp_path: Any) -> AppConfig:
    return replace(make_config(tmp_path, [SBER, SI]), history_check_days=7)


def setup_calendar(iss: IssTapeRepository) -> None:
    week = [(k, 1, None, None) for k in range(1, 8)]
    iss.save_calendar("shares", [("2026-09-13", 0, None, None)], week, "now")
    iss.save_calendar("forts", [("2026-09-13", 0, None, None)], week, "now")


def store_days(repo: MarketDataRepository, days: list[str]) -> None:
    repo.insert_ticks(
        [{"ticker": repo.instrument.storage_name, "timestamp": datetime.fromisoformat(d)
          .replace(hour=10, tzinfo=UTC), "price": 1.0, "volume": 1, "direction": "BUY",
          "trade_id": f"s{d}"} for d in days]
    )  # fmt: skip


def test_calendar_gap_marked(cfg: AppConfig, caplog: pytest.LogCaptureFixture) -> None:
    """SPEC-0013 A-3 (G-2): торговый день без данных — ERROR и calendar_gap; неторговый и
    подтверждённый итогами день без сделок — нет."""
    repos = {i.storage_name: MarketDataRepository(cfg.storage_root, i) for i in cfg.instruments}
    iss = IssTapeRepository(cfg.storage_root)
    setup_calendar(iss)
    store_days(repos["Si_CONTINUOUS"], ["2026-09-11", "2026-09-17", "2026-09-22"])
    store_days(repos["SBER"], ["2026-09-11", "2026-09-22"])
    iss.save_totals("shares", "2026-09-16", {"GAZP": (10, 1.0)})  # у SBER в этот день 0 сделок
    gaps = find_calendar_gaps(cfg, repos, iss, date(2026, 9, 23))
    si = [d.isoformat() for d in gaps["Si_CONTINUOUS"]]
    assert "2026-09-18" in si and "2026-09-19" in si  # будний и выходной без данных
    assert "2026-09-13" not in si  # отменённая сессия выходного дня
    assert "2026-09-16" not in [d.isoformat() for d in gaps["SBER"]]  # подтверждено итогами
    with caplog.at_level(logging.ERROR):
        marked = mark_calendar_gaps(cfg, repos, iss, date(2026, 9, 23))
    assert marked == sum(len(v) for v in gaps.values())
    assert "TRADING_DAY_MISSING Si_CONTINUOUS 2026-09-18" in caplog.text
    assert repos["Si_CONTINUOUS"].days_to_refresh()["2026-09-18"] == REFRESH_CALENDAR_GAP


def test_calendar_gap_not_limited_by_window(cfg: AppConfig) -> None:
    """SPEC-0013 A-3: отметка calendar_gap старше окна history_check_days не снимается."""
    repo = MarketDataRepository(cfg.storage_root, SI)
    old = (datetime.now(UTC) - timedelta(days=40)).strftime("%Y-%m-%d")
    repo.mark_days_for_refresh([old], REFRESH_CALENDAR_GAP)
    assert [d.strftime("%Y-%m-%d") for d in refresh_days(cfg, repo)] == [old]


async def test_maintenance_checks_gaps_before_backfill(
    cfg: AppConfig, monkeypatch: pytest.MonkeyPatch
) -> None:
    """SPEC-0013 A-4: календарь и пропуски — до догрузки, чтобы перекачать этим же проходом."""
    order: list[str] = []

    async def calendar(_config: Any) -> None:
        order.append("calendar")

    def gaps(*_a: Any) -> int:
        order.append("gaps")
        return 0

    async def backfill(*_a: Any, **_k: Any) -> None:
        order.append("backfill")

    async def archiver(*_a: Any, **_k: Any) -> None:
        order.append("archiver")

    async def final(*_a: Any, **_k: Any) -> None:
        order.append("final")

    monkeypatch.setattr(main_mod, "refresh_calendar", calendar)
    monkeypatch.setattr(main_mod.schedule_mod, "mark_calendar_gaps", gaps)
    monkeypatch.setattr(main_mod, "_run_backfill", backfill)
    monkeypatch.setattr(main_mod.archiver_mod, "run_archiver", archiver)
    monkeypatch.setattr(main_mod, "run_final_check_safely", final)
    await main_mod.run_maintenance(cfg, {})
    assert order == ["calendar", "gaps", "backfill", "archiver", "final"]
