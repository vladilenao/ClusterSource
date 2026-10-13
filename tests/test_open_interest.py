"""SPEC-0007: открытый интерес."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import UTC, date, datetime, timedelta
from types import SimpleNamespace
from typing import Any

import pyarrow.parquet as pq
import pytest

from cluster_source.collector import DataCollector
from cluster_source.config import AppConfig, Instrument, InstrumentType
from cluster_source.database import IssTapeRepository, MarketDataRepository
from cluster_source.iss import normalize_trades
from cluster_source.reader import get_contract, get_oi_flow, get_open_interest
from cluster_source.reconcile import Reconciler
from cluster_source.stream_hub import HubConnection, StreamHub

UTC = UTC
SI = Instrument(id="Si", type=InstrumentType.CONTINUOUS_FUTURES, tick_size=1, lot_size=1)
T0 = datetime(2026, 10, 13, 18, 0, tzinfo=UTC)
FORTS_COLUMNS = [
    "TRADENO", "BOARDNAME", "SECID", "TRADEDATE", "TRADETIME", "PRICE", "QUANTITY", "SYSTIME",
    "RECNO", "OPENPOSITION", "OFFMARKETDEAL", "BUYSELL", "TRADE_SESSION_DATE",
]  # fmt: skip


def fut_row(recno: int, sec: int, qty: int, oi: int) -> list[object]:
    msk = T0 + timedelta(hours=3, seconds=sec)
    t = msk.strftime("%H:%M:%S")
    d = msk.strftime("%Y-%m-%d")
    return [recno * 7, "RFUD", "SiZ6", d, t, 85000, qty, f"{d} {t}", recno, oi, 0, "B", d]


def stage(iss: IssTapeRepository, rows: list[list[object]]) -> None:
    iss.stage_rows(normalize_trades("forts", "2026-10-13", FORTS_COLUMNS, rows))


# --------------------------------------------------------------------------- #
def test_subscription_requests_open_interest() -> None:
    """SPEC-0007 A-1: подписка на сделки запрашивает ОИ."""
    pytest.importorskip("t_tech.invest.grpc.schemas")
    from cluster_source.main import build_trades_subscription

    assert build_trades_subscription(["F"]).subscribe_trades_request.with_open_interest is True


async def test_open_interest_routed_by_uid_and_persisted_by_consumer(
    share_config: AppConfig,
) -> None:
    """SPEC-0007 A-1, A-2 (G-3): ОИ по UID доходит до инструмента, в БД пишет консьюмер."""
    repo = MarketDataRepository(share_config.storage_root, SI)
    collector = DataCollector(share_config, SI, repo)

    async def factory(instruments: Sequence[Instrument], resolve: bool) -> HubConnection:
        raise AssertionError

    hub = StreamHub([collector], factory)
    conn = HubConnection(
        stream=_Stream(
            [
                SimpleNamespace(
                    open_interest=SimpleNamespace(
                        instrument_uid="UID_SI", time=T0, open_interest=412920
                    )
                ),
                SimpleNamespace(
                    open_interest=SimpleNamespace(
                        instrument_uid="UID_OTHER", time=T0, open_interest=1
                    )
                ),
            ]
        ),
        routes={"F_SI": "Si_CONTINUOUS"},
        uids={"UID_SI": "Si_CONTINUOUS"},
    )
    hub._running = True
    await hub._pump(conn)
    assert get_open_interest(repo, T0, T0 + timedelta(minutes=1)).empty  # продюсер не пишет
    await collector._flush([])
    df = get_open_interest(repo, T0, T0 + timedelta(minutes=1))
    assert list(df["value"]) == [412920]
    assert df["ts"].iloc[0] == T0


class _Stream:
    def __init__(self, items: list[Any]) -> None:
        self._items = list(items)

    def __aiter__(self) -> _Stream:
        return self

    async def __anext__(self) -> Any:
        if not self._items:
            raise StopAsyncIteration
        item = self._items.pop(0)
        for attr in ("trade", "subscribe_trades_response", "subscribe_info_response"):
            if not hasattr(item, attr):
                setattr(item, attr, None)
        if not hasattr(item, "trading_status"):
            item.trading_status = None
        return item


def test_open_interest_purged_with_hot_store(share_config: AppConfig) -> None:
    repo = MarketDataRepository(share_config.storage_root, SI)
    repo.insert_open_interest([(T0 - timedelta(days=10), 1), (T0, 2)])
    repo.purge_hot_before(T0 - timedelta(days=7))
    assert list(
        get_open_interest(repo, T0 - timedelta(days=30), T0 + timedelta(days=1))["value"]
    ) == [2]


# --------------------------------------------------------------------------- #
def test_recno_kept_through_parquet(share_config: AppConfig) -> None:
    """SPEC-0007 A-4: RECNO сохраняется в ленте и в Parquet."""
    iss = IssTapeRepository(share_config.storage_root)
    stage(iss, [fut_row(5, 0, 1, 100), fut_row(6, 1, 1, 102)])
    path = iss.finalize_feed_day("forts", "2026-10-13")
    assert path is not None
    assert pq.read_table(path).column("recno").to_pylist() == [5, 6]


def test_oi_flow_classification(share_config: AppConfig) -> None:
    """SPEC-0007 A-3: ΔОИ = ±2 × количество; открыто / закрыто / передано / вне ленты."""
    iss = IssTapeRepository(share_config.storage_root)
    stage(
        iss,
        [
            fut_row(1, -30, 1, 100),  # до окна: ОИ до первой сделки
            fut_row(2, 1, 2, 104),  # +4 = 2 × 2 → открыто 2
            fut_row(3, 2, 3, 98),  # −6 → закрыто 3
            fut_row(4, 3, 5, 98),  # 0 → передано 5
            fut_row(5, 4, 4, 100),  # +2 при q=4 → открыто 1, передано 3
            fut_row(6, 65, 1, 110),  # +10 при q=1 → открыто 1, вне ленты 4 (следующий бар)
        ],
    )
    bars = get_oi_flow(iss, "SiZ6", T0, T0 + timedelta(minutes=2))
    first, second = bars.iloc[0], bars.iloc[1]
    assert (first["opened"], first["closed"], first["transferred"], first["outside"]) == (
        3,
        3,
        8,
        0,
    )
    assert (first["oi_close"], first["oi_change"], first["trades"]) == (100, 0, 4)
    assert (second["opened"], second["outside"], second["oi_close"]) == (1, 4, 110)


def test_oi_flow_order_by_recno_not_insertion(share_config: AppConfig) -> None:
    """SPEC-0007 A-4: порядок сделок — RECNO, даже если строки вставлены иначе."""
    iss = IssTapeRepository(share_config.storage_root)
    stage(iss, [fut_row(3, 2, 1, 104), fut_row(1, -30, 1, 100), fut_row(2, 1, 1, 102)])
    bars = get_oi_flow(iss, "SiZ6", T0, T0 + timedelta(minutes=1))
    assert (bars.iloc[0]["opened"], bars.iloc[0]["closed"]) == (2, 0)


# --------------------------------------------------------------------------- #
def test_reconciler_records_active_contract(share_config: AppConfig) -> None:
    """SPEC-0007 A-5: сверка записывает активный контракт непрерывного фьючерса по дням."""
    repo = MarketDataRepository(share_config.storage_root, SI)
    iss = IssTapeRepository(share_config.storage_root)
    iss.start_feed("forts", "1|x", "2026-10-13")
    stage(iss, [fut_row(1, -60, 1, 100), fut_row(2, 60 * 30, 1, 102)])
    rec = Reconciler(
        share_config, {"Si_CONTINUOUS": repo}, iss, lambda _i: "SiZ6",
        now=lambda: T0 + timedelta(hours=1),
    )  # fmt: skip
    rec.reconcile_instrument(SI)
    assert get_contract(repo, date(2026, 10, 13)) == "SiZ6"
    assert get_contract(repo, date(2026, 10, 12)) is None
