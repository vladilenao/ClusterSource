"""SPEC-0014: хранилища ближайших контрактов непрерывного фьючерса (G-3, G-5)."""

from __future__ import annotations

from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from test_final import FakeIss, get_day_verdicts, ticks
from test_stream_hub import ListStream, frame, trade

from cluster_source import autoload
from cluster_source.autoload import (
    REFRESH_CONTRACT_BACKFILL,
    REFRESH_DEALER_CLEANUP,
    days_to_load,
    refresh_days,
    schedule_contract_backfill,
)
from cluster_source.collector import DataCollector
from cluster_source.config import AppConfig, FuturesMeta, Instrument, InstrumentType
from cluster_source.contracts import contract_storages, with_storages
from cluster_source.database import IssTapeRepository, MarketDataRepository
from cluster_source.final import STATUS_FINAL, run_final_check
from cluster_source.iss import KIND_FEED, STATUS_COMPLETE
from cluster_source.quality import NORMAL_TRADING, REASON_STREAM_SILENCE
from cluster_source.schedule import find_calendar_gaps
from cluster_source.stream_hub import HubConnection, StreamHub

SI = Instrument(id="Si", type=InstrumentType.CONTINUOUS_FUTURES, tick_size=1, lot_size=1)
BR = Instrument(
    id="BR", type=InstrumentType.CONTINUOUS_FUTURES, tick_size=0.01, lot_size=10, contracts=3
)


def meta(ticker: str, last: date) -> FuturesMeta:
    return FuturesMeta(ticker, "SPBFUT", last + timedelta(days=1), f"F_{ticker}", last_trade=last)


FUTURES = [
    meta("SiU6", date(2026, 9, 17)),
    meta("SiZ6", date(2026, 12, 17)),
    meta("SiH7", date(2027, 3, 18)),
    meta("SiM7", date(2027, 6, 17)),
    meta("BRV6", date(2026, 10, 1)),
    meta("BRX6", date(2026, 11, 2)),
    meta("BRZ6", date(2026, 12, 1)),
    meta("BRF7", date(2026, 12, 29)),
    meta("BRG7", date(2027, 1, 29)),
    meta("BRH7", date(2027, 3, 1)),
]
FIRST = {"Si_CONTINUOUS": date(2026, 8, 8), "BR_CONTINUOUS": date(2026, 8, 8)}
TODAY = date(2026, 11, 17)


def storages(config: AppConfig, today: date = TODAY) -> dict[str, Instrument]:
    return {i.id: i for i in contract_storages(config, FUTURES, FIRST, today)}


@pytest.fixture
def cfg(share_config: AppConfig) -> AppConfig:
    return replace(share_config, instruments=[SI, BR])


# --------------------------------------------------------------------------- #
def test_contract_window(cfg: AppConfig, tmp_path: Path) -> None:
    """SPEC-0014 A-8: N ближайших по последнему дню торгов; ``contracts`` актива важнее
    ``futures_contracts``; N = 1 и конфликт с явным futures_contract — ошибка."""
    got = storages(cfg)
    si = {k: v.window for k, v in got.items() if k.startswith("Si")}
    assert si == {
        "SiU6": (date(2026, 8, 8), date(2026, 9, 17)),
        "SiZ6": (date(2026, 8, 8), date(2026, 12, 17)),
        "SiH7": (date(2026, 9, 18), date(2027, 3, 18)),
    }  # SiM7 войдёт 18.12 — после сегодняшнего дня
    br = {k: v.window for k, v in got.items() if k.startswith("BR")}
    assert br == {
        "BRV6": (date(2026, 8, 8), date(2026, 10, 1)),
        "BRX6": (date(2026, 8, 8), date(2026, 11, 2)),
        "BRZ6": (date(2026, 8, 8), date(2026, 12, 1)),
        "BRF7": (date(2026, 10, 2), date(2026, 12, 29)),
        "BRG7": (date(2026, 11, 3), date(2027, 1, 29)),
    }  # 3 ближайших у BR
    h7 = got["SiH7"]
    assert (h7.storage_name, h7.type, h7.underlying, h7.tick_size) == (
        "SiH7",
        InstrumentType.FUTURES_CONTRACT,
        "Si",
        1,
    )
    assert [i.id for i in with_storages(cfg, got.values()).instruments][:2] == ["Si", "BR"]
    assert storages(replace(cfg, futures_contracts=0, instruments=[SI])) == {}

    yaml_text = "token: t\ntickers:\n  - id: Si\n    type: continuous_futures\n"
    path = tmp_path / "c.yaml"
    path.write_text(yaml_text + "    contracts: 3\n", encoding="utf-8")
    loaded = AppConfig.from_yaml(path)
    assert loaded.contracts_of(loaded.instruments[0]) == 3 and loaded.futures_contracts == 2
    path.write_text(yaml_text + "    contracts: 1\n", encoding="utf-8")
    with pytest.raises(ValueError, match="contracts"):
        AppConfig.from_yaml(path)
    explicit = Instrument(id="SiZ6", type=InstrumentType.FUTURES_CONTRACT, tick_size=1, lot_size=1)
    with pytest.raises(ValueError, match="SiZ6"):
        storages(replace(cfg, instruments=[SI, explicit]))


@pytest.fixture
def hub_parts(cfg: AppConfig) -> dict[str, DataCollector]:
    got = storages(cfg)
    out: dict[str, DataCollector] = {}
    for instr in (SI, got["SiZ6"], got["SiH7"]):
        repo = MarketDataRepository(cfg.storage_root, instr)
        out[instr.storage_name] = DataCollector(cfg, instr, repo)
    return out


def _queued(collector: DataCollector) -> list[float]:
    return [t.price for t in list(collector._queue._queue)]  # type: ignore[attr-defined]


def _connection(stream: Any) -> HubConnection:
    return HubConnection(
        stream=stream,
        routes={"F_SiZ6": "Si_CONTINUOUS", "F_SiH7": "SiH7"},
        uids={"U_SiZ6": "Si_CONTINUOUS"},
        mirrors={"Si_CONTINUOUS": ["SiZ6"]},
    )


async def test_hub_routes_figi_to_many(hub_parts: dict[str, DataCollector]) -> None:
    """SPEC-0014 A-9 (G-3): сделка и ОИ FIGI/UID активного контракта — в непрерывный и в
    хранилище контракта; второй контракт — только в своё."""
    oi = SimpleNamespace(instrument_uid="U_SiZ6", open_interest=1234, time=datetime.now(UTC))
    stream = ListStream(
        [
            frame(trade=trade("F_SiZ6", 1.0)),
            frame(trade=trade("F_SiH7", 2.0)),
            frame(open_interest=oi),
        ]
    )
    hub_ref: list[StreamHub] = []
    streams = [stream]

    async def factory(_instruments: Any, _resolve: bool) -> HubConnection:
        if not streams:
            await hub_ref[0].shutdown()
            return _connection(ListStream([]))
        return _connection(streams.pop(0))

    hub = StreamHub(list(hub_parts.values()), factory)
    hub_ref.append(hub)
    await __import__("asyncio").wait_for(hub.run(), timeout=5)
    assert _queued(hub_parts["Si_CONTINUOUS"]) == [1.0]
    assert _queued(hub_parts["SiZ6"]) == [1.0]
    assert _queued(hub_parts["SiH7"]) == [2.0]
    assert [v for _t, v in hub_parts["SiZ6"]._pending_oi] == [1234]  # type: ignore[attr-defined]
    assert [v for _t, v in hub_parts["Si_CONTINUOUS"]._pending_oi] == [1234]  # type: ignore[attr-defined]


def test_inactive_contract_no_silence(hub_parts: dict[str, DataCollector]) -> None:
    """SPEC-0014 A-11 (G-5): у неактивного контракта нет ``stream_silence``; активный
    (зеркало непрерывного) и остальные условия — как у всех."""

    async def factory(*_: Any) -> HubConnection:
        raise AssertionError

    hub = StreamHub(list(hub_parts.values()), factory)
    t0 = datetime(2026, 11, 17, 8, 0, tzinfo=UTC)
    hub._on_opened(_connection(ListStream([])))  # type: ignore[attr-defined]
    for collector in hub_parts.values():
        collector.listening_since = t0
        collector.set_trading_status(NORMAL_TRADING, t0)
    hub.check(t0 + timedelta(hours=2))
    assert not hub_parts["SiH7"].quality.is_active(REASON_STREAM_SILENCE)
    assert hub_parts["SiZ6"].quality.is_active(REASON_STREAM_SILENCE)
    assert hub_parts["Si_CONTINUOUS"].quality.is_active(REASON_STREAM_SILENCE)


def _tape(iss: IssTapeRepository, secid: str, day: date, n: int) -> None:
    rows = [
        {
            "market": "forts", "feed_date": day.isoformat(), "secid": secid, "board": "RFUD",
            "tradeno": int(day.strftime("%Y%m%d")) * 100 + k, "trade_date": day.isoformat(),
            "timestamp": f"{day}T08:00:0{k}.000000+00:00", "price": 1.0, "quantity": 1,
            "direction": "BUY", "period": None, "session": None, "open_position": 1,
            "off_market": 0,
        }
        for k in range(n)
    ]  # fmt: skip
    iss.stage_rows(rows)
    iss.set_day_status("forts", day.isoformat(), KIND_FEED, STATUS_COMPLETE, "")


async def test_contract_storage_lifecycle(cfg: AppConfig, monkeypatch: pytest.MonkeyPatch) -> None:
    """SPEC-0014 A-10 (G-3): догрузка и пропуски — только в окне, FINAL — по своему secid;
    после окна хранилище остаётся в обслуживании, но не собирается стримом."""
    got = storages(cfg)
    h7, u6 = got["SiH7"], got["SiU6"]
    assert not u6.in_window(TODAY) and h7.in_window(TODAY)  # U6 — без подписки
    extended = with_storages(replace(cfg, history_check_days=5), [h7, u6])
    repos = {
        i.storage_name: MarketDataRepository(cfg.storage_root, i) for i in extended.instruments
    }
    iss = IssTapeRepository(cfg.storage_root)

    monkeypatch.setattr(autoload, "_utc_today", lambda: datetime(2026, 9, 20, tzinfo=UTC))
    missing, _ = days_to_load(extended, repos["SiH7"])
    assert sorted(d.date() for d in missing) == [date(2026, 9, d) for d in (18, 19, 20)]

    # Пропуски SiU6: 16.09 у ISS были сделки — пропуск; 15.09 — 0 сделок; 18.09 — вне окна.
    for day, n in ((date(2026, 9, 15), 0), (date(2026, 9, 16), 7), (date(2026, 9, 18), 9)):
        iss.save_totals("forts", day.isoformat(), {"SiU6": (n, float(n)), "SiZ6": (1, 1.0)})
    repos["SiU6"].insert_ticks(ticks("SiU6", date(2026, 9, 14), 1))
    gaps = find_calendar_gaps(extended, repos, iss, date(2026, 9, 20))
    assert gaps.get("SiU6") == [date(2026, 9, 16)]

    # FINAL — по ленте своего secid; день вне окна вердикта не получает.
    d = date(2026, 10, 13)
    repos["SiH7"].insert_ticks(ticks("SiH7", d, 3))
    _tape(iss, "SiH7", d, 3)
    fake = FakeIss(
        {("forts", d.isoformat()): {"SiH7": (3, 3.0), "SiU6": (1, 1.0)}},
        {"SiH7": [("2026-10-13 11:00:00", 3.0, 100.0, 100.0)]},
    )
    futures = {m.ticker: m for m in FUTURES}
    await run_final_check(extended, repos, iss, fake, futures, date(2026, 10, 20))
    row = get_day_verdicts(repos["SiH7"]).set_index("trade_date").loc["2026-10-13"]
    assert (row["status"], row["reference"]) == (STATUS_FINAL, "iss_tape")
    assert "SiH7" in row["detail"]
    assert "2026-10-13" not in set(get_day_verdicts(repos["SiU6"])["trade_date"])


async def test_inactive_contract_empty_day(cfg: AppConfig) -> None:
    """SPEC-0014 A-12 (G-5): день без сделок по итогам ISS (и выходной без них в ленте) —
    не пропуск, а FINAL «сделок не было»; день со сделками у ISS и без наших — пропуск."""
    h7 = storages(cfg)["SiH7"]
    extended = with_storages(cfg, [h7])
    repos = {
        i.storage_name: MarketDataRepository(cfg.storage_root, i) for i in extended.instruments
    }
    iss = IssTapeRepository(cfg.storage_root)
    repos["Si_CONTINUOUS"].insert_ticks(ticks("Si_CONTINUOUS", date(2026, 11, 6), 1))
    # Пн 09.11 — 0 сделок SiH7; вт 10.11 — 4 сделки, у нас нет; сб 07.11 — в ленте нет.
    iss.save_totals("forts", "2026-11-09", {"SiZ6": (100, 100.0)})
    iss.save_totals("forts", "2026-11-10", {"SiZ6": (100, 100.0), "SiH7": (4, 4.0)})
    _tape(iss, "SiZ6", date(2026, 11, 9), 2)
    gaps = find_calendar_gaps(extended, repos, iss, date(2026, 11, 11))
    assert gaps.get("SiH7") == [date(2026, 11, 10)]

    fake = FakeIss(
        {
            ("forts", "2026-11-06"): {"SiZ6": (1, 1.0)},
            ("forts", "2026-11-09"): {"SiZ6": (100, 100.0)},
        }
    )
    await run_final_check(extended, repos, iss, fake, {m.ticker: m for m in FUTURES}, TODAY)
    df = get_day_verdicts(repos["SiH7"]).set_index("trade_date")
    assert df.loc["2026-11-09", "status"] == STATUS_FINAL
    assert df.loc["2026-11-09", "detail"] == "SiH7: сделок не было"


def test_contract_initial_backfill_background(
    cfg: AppConfig, monkeypatch: pytest.MonkeyPatch
) -> None:
    """SPEC-0014 A-13: окно хранилища догружается фоновым проходом (``cleanup=True``),
    ночное обслуживание его не ждёт; такие дни — не пропуски по календарю."""
    monkeypatch.setattr(autoload, "_utc_today", lambda: datetime(2026, 9, 22, tzinfo=UTC))
    h7 = storages(cfg, date(2026, 9, 22))["SiH7"]
    extended = with_storages(cfg, [h7])
    repo = MarketDataRepository(cfg.storage_root, h7)
    repos = {
        i.storage_name: MarketDataRepository(cfg.storage_root, i) for i in extended.instruments
    }
    repos["SiH7"] = repo
    repo.insert_ticks(ticks("SiH7", date(2026, 9, 19), 1))
    assert schedule_contract_backfill(repo) == 3  # 18, 20, 21.09 (19.09 уже есть)
    assert schedule_contract_backfill(repo) == 0  # однократно
    assert set(repo.days_to_refresh().values()) == {REFRESH_CONTRACT_BACKFILL}
    assert [d.date() for d in refresh_days(extended, repo, cleanup=True)] == [
        date(2026, 9, 21),
        date(2026, 9, 20),
        date(2026, 9, 18),
    ]
    missing, refresh = days_to_load(extended, repo)
    assert refresh == [] and {d.date() for d in missing} == {date(2026, 9, 22)}

    iss = IssTapeRepository(cfg.storage_root)
    iss.save_totals("forts", "2026-09-21", {"SiH7": (5, 5.0)})
    assert "SiH7" not in find_calendar_gaps(extended, repos, iss, date(2026, 9, 22))
    # Хранилище с данными до SPEC-0002 чистится; новое пустое — помечено как чистое.
    empty = MarketDataRepository(cfg.storage_root, storages(cfg, date(2026, 9, 22))["SiZ6"])
    schedule_contract_backfill(empty)
    assert REFRESH_DEALER_CLEANUP not in set(empty.days_to_refresh().values())
    assert empty.get_meta(autoload.DEALER_CLEANUP_META_KEY) is not None
