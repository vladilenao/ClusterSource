"""Тесты репозитория (горячее SQLite + холодное Parquet)."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pandas as pd
import pytest

from cluster_source.config import AppConfig, Instrument
from cluster_source.database import DataCorruptionError, MarketDataRepository

UTC = UTC


def _tick(day_offset: int, ts_suffix: str, price: float, direction: str = "BUY") -> dict:
    base = datetime.now(UTC).replace(hour=0, minute=0, second=0, microsecond=0)
    ts = (
        base
        + timedelta(days=day_offset)
        + timedelta(hours=int(ts_suffix[:2]), minutes=int(ts_suffix[3:]))
    )
    return {
        "ticker": "SBER",
        "timestamp": ts,
        "price": price,
        "volume": 10,
        "direction": direction,
        "trade_id": f"{ts_suffix}{price}",
    }


def test_insert_and_read_roundtrip(share_repo: MarketDataRepository) -> None:
    now = datetime.now(UTC).replace(hour=12, minute=0, second=0, microsecond=0)
    rows = [
        {
            "ticker": "SBER",
            "timestamp": now,
            "price": 312.5,
            "volume": 5,
            "direction": "BUY",
            "trade_id": "1",
        },
        {
            "ticker": "SBER",
            "timestamp": now + timedelta(seconds=1),
            "price": 312.6,
            "volume": 7,
            "direction": "SELL",
            "trade_id": "2",
        },
    ]
    share_repo.insert_ticks(rows)
    df = share_repo.read_ticks(now - timedelta(minutes=5), now + timedelta(minutes=5))
    assert len(df) == 2
    assert df["price"].tolist() == [312.5, 312.6]
    assert df["timestamp"].dt.tz is not None


def test_hot_days_available(share_repo: MarketDataRepository) -> None:
    today = datetime.now(UTC).replace(hour=10, minute=0, second=0, microsecond=0)
    share_repo.insert_ticks(
        [
            {
                "ticker": "SBER",
                "timestamp": today,
                "price": 100.0,
                "volume": 1,
                "direction": "BUY",
                "trade_id": "1",
            },
            {
                "ticker": "SBER",
                "timestamp": today - timedelta(days=2),
                "price": 99.0,
                "volume": 1,
                "direction": "SELL",
                "trade_id": "2",
            },
        ]
    )
    days = share_repo.hot_days_available()
    assert days == {(today).strftime("%Y-%m-%d"), (today - timedelta(days=2)).strftime("%Y-%m-%d")}


def test_write_parquet_and_read_back(share_repo: MarketDataRepository) -> None:
    day = datetime.now(UTC).replace(hour=0, minute=0, second=0, microsecond=0)
    df = pd.DataFrame(
        {
            "ticker": ["SBER"] * 2,
            "timestamp": [day + timedelta(hours=1), day + timedelta(hours=2)],
            "price": [100.0, 101.0],
            "volume": [3, 4],
            "direction": ["BUY", "SELL"],
            "trade_id": ["1", "2"],
        }
    )
    path = share_repo.write_parquet(day.strftime("%Y-%m-%d"), df)
    assert path.exists() and path.stat().st_size > 0

    read = share_repo.read_parquet(day, day + timedelta(days=1))
    assert len(read) == 2
    assert sorted(read["price"].tolist()) == [100.0, 101.0]


def test_write_parquet_raises_on_empty(share_repo: MarketDataRepository) -> None:
    with pytest.raises(DataCorruptionError):
        share_repo.write_parquet("2026-01-01", pd.DataFrame())


def test_purge_hot_before(share_repo: MarketDataRepository) -> None:
    old = datetime.now(UTC).replace(hour=10, minute=0, second=0, microsecond=0) - timedelta(days=10)
    fresh = old + timedelta(days=9)
    share_repo.insert_ticks(
        [
            {
                "ticker": "SBER",
                "timestamp": old,
                "price": 1.0,
                "volume": 1,
                "direction": "BUY",
                "trade_id": "1",
            },
            {
                "ticker": "SBER",
                "timestamp": fresh,
                "price": 2.0,
                "volume": 1,
                "direction": "SELL",
                "trade_id": "2",
            },
        ]
    )
    share_repo.purge_hot_before(datetime.now(UTC) - timedelta(days=7))
    assert share_repo.count_hot() == 1


def test_wal_enabled(share_repo: MarketDataRepository) -> None:
    import sqlite3

    # Вызываем сессию, чтобы режим WAL закрепился в файле БД.
    share_repo.insert_ticks(
        [
            {
                "ticker": "SBER",
                "timestamp": datetime.now(UTC),
                "price": 1.0,
                "volume": 1,
                "direction": "BUY",
                "trade_id": "1",
            }
        ]
    )
    conn = sqlite3.connect(share_repo._db_path)
    mode = conn.execute("PRAGMA journal_mode").fetchone()[0]
    conn.close()
    assert mode.upper() == "WAL"


def test_available_days_union(share_config: AppConfig) -> None:
    instr: Instrument = share_config.instruments[0]
    repo = MarketDataRepository(share_config.storage_root, instr)

    hot_day = datetime.now(UTC).replace(hour=10, minute=0, second=0, microsecond=0)
    repo.insert_ticks(
        [
            {
                "ticker": "SBER",
                "timestamp": hot_day,
                "price": 1.0,
                "volume": 1,
                "direction": "BUY",
                "trade_id": "1",
            }
        ]
    )
    cold_day = hot_day - timedelta(days=20)
    df = pd.DataFrame(
        {
            "ticker": ["SBER"],
            "timestamp": [cold_day],
            "price": [1.0],
            "volume": [1],
            "direction": ["BUY"],
            "trade_id": ["x"],
        }
    )
    repo.write_parquet(cold_day.strftime("%Y-%m-%d"), df)

    days = repo.available_days()
    assert hot_day.strftime("%Y-%m-%d") in days
    assert cold_day.strftime("%Y-%m-%d") in days
