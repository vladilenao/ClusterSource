"""Тесты ридера / footprint."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pandas as pd

from cluster_source.config import AppConfig, Instrument, InstrumentType
from cluster_source.database import MarketDataRepository
from cluster_source.reader import get_cluster_data

UTC = UTC


def _build_repo(share_config: AppConfig) -> MarketDataRepository:
    return MarketDataRepository(share_config.storage_root, share_config.instruments[0])


def test_get_cluster_data_merges_hot_and_cold(share_config: AppConfig) -> None:
    repo = _build_repo(share_config)
    now = datetime.now(UTC).replace(hour=12, minute=0, second=0, microsecond=0)

    # горячие тики (недавние)
    repo.insert_ticks(
        [
            {
                "ticker": "SBER",
                "timestamp": now,
                "price": 99.99,
                "volume": 5,
                "direction": "BUY",
                "trade_id": "1",
            },
            {
                "ticker": "SBER",
                "timestamp": now + timedelta(seconds=30),
                "price": 100.0,
                "volume": 3,
                "direction": "SELL",
                "trade_id": "2",
            },
        ]
    )
    # холодные тики (старше 7 дней)
    cold = now - timedelta(days=20)
    df = pd.DataFrame(
        {
            "ticker": ["SBER"],
            "timestamp": [cold],
            "price": [101.0],
            "volume": [7],
            "direction": ["BUY"],
            "trade_id": ["x"],
        }
    )
    repo.write_parquet(cold.strftime("%Y-%m-%d"), df)

    result = get_cluster_data(
        share_config,
        repo,
        "SBER",
        start_time=now - timedelta(days=21),
        end_time=now + timedelta(hours=1),
        timeframe="5min",
    )
    assert not result.empty
    assert {"price", "buy_volume", "sell_volume", "delta"}.issubset(result.columns)


def test_hot_and_cold_overlap_not_double_counted(share_config: AppConfig) -> None:
    """День, заархивированный в холод, остаётся в горячем окне — идентичные
    строки из обоих хранилищ должны дедуплицироваться, а не суммироваться
    дважды."""
    repo = _build_repo(share_config)
    now = datetime.now(UTC).replace(hour=12, minute=0, second=0, microsecond=0)

    # Одна и та же строка в обоих хранилищах (горячее окно хранит её после архивации)
    rows = [
        {
            "ticker": "SBER",
            "timestamp": now,
            "price": 100.0,
            "volume": 10,
            "direction": "BUY",
            "trade_id": "1",
        },
        {
            "ticker": "SBER",
            "timestamp": now,
            "price": 101.0,
            "volume": 5,
            "direction": "SELL",
            "trade_id": "2",
        },
    ]
    repo.insert_ticks([dict(r, timestamp=now if r["trade_id"] == "1" else now) for r in rows])
    df = pd.DataFrame(rows)
    repo.write_parquet(now.strftime("%Y-%m-%d"), df)

    result = get_cluster_data(
        share_config,
        repo,
        "SBER",
        start_time=now - timedelta(minutes=5),
        end_time=now + timedelta(minutes=5),
    )
    row_100 = result[result["price"] == 100.0].iloc[0]
    row_101 = result[result["price"] == 101.0].iloc[0]
    assert row_100["buy_volume"] == 10
    assert row_101["sell_volume"] == 5


def test_prices_rounded_to_tick_size(share_config: AppConfig) -> None:
    repo = _build_repo(share_config)
    now = datetime.now(UTC).replace(hour=12, minute=0, second=0, microsecond=0)
    repo.insert_ticks(
        [
            {
                "ticker": "SBER",
                "timestamp": now,
                "price": 312.514,
                "volume": 2,
                "direction": "BUY",
                "trade_id": "1",
            },
            {
                "ticker": "SBER",
                "timestamp": now,
                "price": 312.499,
                "volume": 2,
                "direction": "BUY",
                "trade_id": "2",
            },
        ]
    )
    result = get_cluster_data(
        share_config,
        repo,
        "SBER",
        start_time=now - timedelta(minutes=5),
        end_time=now + timedelta(minutes=5),
    )
    prices = sorted(result["price"].unique().tolist())
    assert prices == [312.5, 312.51]


def test_delta_split(share_config: AppConfig) -> None:
    repo = _build_repo(share_config)
    now = datetime.now(UTC).replace(hour=12, minute=0, second=0, microsecond=0)
    repo.insert_ticks(
        [
            {
                "ticker": "SBER",
                "timestamp": now,
                "price": 100.0,
                "volume": 10,
                "direction": "BUY",
                "trade_id": "1",
            },
            {
                "ticker": "SBER",
                "timestamp": now,
                "price": 100.0,
                "volume": 4,
                "direction": "SELL",
                "trade_id": "2",
            },
            {
                "ticker": "SBER",
                "timestamp": now,
                "price": 101.0,
                "volume": 6,
                "direction": "SELL",
                "trade_id": "3",
            },
        ]
    )
    result = get_cluster_data(
        share_config,
        repo,
        "SBER",
        start_time=now - timedelta(minutes=5),
        end_time=now + timedelta(minutes=5),
    )
    row_100 = result[result["price"] == 100.0].iloc[0]
    row_101 = result[result["price"] == 101.0].iloc[0]
    assert row_100["buy_volume"] == 10 and row_100["sell_volume"] == 4 and row_100["delta"] == 6
    assert row_101["buy_volume"] == 0 and row_101["sell_volume"] == 6 and row_101["delta"] == -6


def test_continuous_futures_rounding(share_config: AppConfig) -> None:
    si = Instrument(id="Si", type=InstrumentType.CONTINUOUS_FUTURES, tick_size=1.0, lot_size=1)
    cfg = AppConfig(token=share_config.token, data_dir=share_config.data_dir, instruments=[si])
    repo = MarketDataRepository(cfg.storage_root, si)
    now = datetime.now(UTC).replace(hour=12, minute=0, second=0, microsecond=0)
    repo.insert_ticks(
        [
            {
                "ticker": "Si_CONTINUOUS",
                "timestamp": now,
                "price": 91.4,
                "volume": 1,
                "direction": "BUY",
                "trade_id": "1",
            },
            {
                "ticker": "Si_CONTINUOUS",
                "timestamp": now,
                "price": 90.6,
                "volume": 1,
                "direction": "BUY",
                "trade_id": "2",
            },
        ]
    )
    result = get_cluster_data(
        cfg,
        repo,
        "Si_CONTINUOUS",
        start_time=now - timedelta(minutes=5),
        end_time=now + timedelta(minutes=5),
    )
    assert sorted(result["price"].unique().tolist()) == [91.0]


def test_empty_interval_returns_empty_frame(share_config: AppConfig) -> None:
    """KI-14: нет данных — пустой DataFrame с колонками результата, без исключения."""
    repo = _build_repo(share_config)
    start = datetime(2026, 1, 1, tzinfo=UTC)
    result = get_cluster_data(share_config, repo, "SBER", start, start + timedelta(days=1))
    assert result.empty
    assert list(result.columns) == ["timestamp", "price", "buy_volume", "sell_volume", "delta"]


def test_identical_trades_are_not_collapsed(share_config: AppConfig) -> None:
    """Две реальные сделки с одинаковыми полями (одна микросекунда) учитываются обе."""
    repo = _build_repo(share_config)
    now = datetime.now(UTC).replace(hour=12, minute=0, second=0, microsecond=0)
    row = {
        "ticker": "SBER",
        "timestamp": now,
        "price": 100.0,
        "volume": 1,
        "direction": "BUY",
        "trade_id": "s20260101120000000000",
    }
    repo.insert_ticks([row, dict(row)])
    result = get_cluster_data(
        share_config, repo, "SBER", now - timedelta(minutes=5), now + timedelta(minutes=5)
    )
    assert result["buy_volume"].sum() == 2


def test_cold_end_day_included_and_bounds_respected(share_config: AppConfig) -> None:
    """KI-05: Parquet-день end_time читается, строки вне интервала не учитываются."""
    repo = _build_repo(share_config)
    d1 = datetime(2026, 9, 3, tzinfo=UTC)

    def frame(ts: datetime, volume: int) -> pd.DataFrame:
        return pd.DataFrame(
            {
                "ticker": ["SBER"],
                "timestamp": [ts],
                "price": [100.0],
                "volume": [volume],
                "direction": ["BUY"],
                "trade_id": [str(volume)],
            }
        )

    repo.write_parquet("2026-09-03", frame(d1 + timedelta(hours=8), 1))  # до start
    repo.write_parquet("2026-09-04", frame(d1 + timedelta(days=1, hours=9), 10))
    result = get_cluster_data(
        share_config,
        repo,
        "SBER",
        start_time=d1 + timedelta(hours=12),
        end_time=d1 + timedelta(days=1, hours=12),
        timeframe="1h",
    )
    assert result["buy_volume"].sum() == 10


def test_unknown_direction_excluded_from_sides(share_config: AppConfig) -> None:
    repo = _build_repo(share_config)
    now = datetime.now(UTC).replace(hour=12, minute=0, second=0, microsecond=0)
    repo.insert_ticks(
        [
            {
                "ticker": "SBER",
                "timestamp": now,
                "price": 100.0,
                "volume": 7,
                "direction": "UNKNOWN",
                "trade_id": "1",
            }
        ]
    )
    result = get_cluster_data(
        share_config, repo, "SBER", now - timedelta(minutes=5), now + timedelta(minutes=5)
    )
    assert result["buy_volume"].sum() == 0 and result["sell_volume"].sum() == 0
