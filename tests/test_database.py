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


def _row(ts: datetime, tid: str) -> dict:
    return {
        "ticker": "SBER",
        "timestamp": ts,
        "price": 1.0,
        "volume": 1,
        "direction": "BUY",
        "trade_id": tid,
    }


def test_replace_hot_day_only_touches_that_day(share_repo: MarketDataRepository) -> None:
    day = datetime(2026, 9, 3, tzinfo=UTC)
    share_repo.insert_ticks(
        [_row(day + timedelta(hours=1), "old"), _row(day + timedelta(days=1, hours=1), "next")]
    )
    share_repo.replace_hot_day("2026-09-03", [_row(day + timedelta(hours=2), "new")])
    df = share_repo.read_ticks(day, day + timedelta(days=2))
    assert sorted(df["trade_id"]) == ["new", "next"]


def test_refresh_marks_roundtrip(share_repo: MarketDataRepository) -> None:
    share_repo.mark_days_for_refresh(["2026-09-03"], "compromised")
    share_repo.mark_days_for_refresh(["2026-09-03", "2026-09-04"], "stream_gap")
    assert share_repo.days_to_refresh() == {
        "2026-09-03": "compromised",  # первая причина сохраняется
        "2026-09-04": "stream_gap",
    }
    share_repo.clear_refresh_mark("2026-09-03")
    assert share_repo.days_to_refresh() == {"2026-09-04": "stream_gap"}


def test_read_parquet_filters_by_exact_time(share_repo: MarketDataRepository) -> None:
    """KI-05: день end_time читается, строки вне [start, end) отбрасываются."""
    d1 = datetime(2026, 9, 3, tzinfo=UTC)
    d2 = d1 + timedelta(days=1)
    share_repo.write_parquet(
        "2026-09-03",
        pd.DataFrame([_row(d1 + timedelta(hours=8), "early"), _row(d1 + timedelta(hours=14), "a")]),
    )
    share_repo.write_parquet(
        "2026-09-04",
        pd.DataFrame([_row(d2 + timedelta(hours=9), "b"), _row(d2 + timedelta(hours=15), "late")]),
    )
    df = share_repo.read_parquet(d1 + timedelta(hours=12), d2 + timedelta(hours=12))
    assert sorted(df["trade_id"]) == ["a", "b"]
    same_day = share_repo.read_parquet(d2 + timedelta(hours=8), d2 + timedelta(hours=10))
    assert same_day["trade_id"].tolist() == ["b"]


def test_read_ticks_mixed_timestamp_formats(share_repo: MarketDataRepository) -> None:
    """Строки с микросекундами и без них (старые данные) читаются вместе."""
    base = datetime(2026, 10, 11, 6, 59, 19, tzinfo=UTC)
    share_repo.insert_ticks([_row(base, "a"), _row(base + timedelta(microseconds=5), "b")])
    with share_repo.session() as conn:  # имитируем строку старого формата без дробной части
        conn.execute(
            "UPDATE ticks_hot_store SET timestamp = ? WHERE trade_id = 'a'",
            ("2026-10-11T06:59:19+00:00",),
        )
        conn.commit()
    df = share_repo.hot_rows_for_day(base)
    assert len(df) == 2
    assert df["timestamp"].dt.tz is not None
