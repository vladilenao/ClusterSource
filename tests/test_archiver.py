"""Тесты архиватора: горячее->холодное архивирование, контроль валидации, очистка."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pandas as pd  # noqa: F401

from cluster_source.archiver import DataArchiver, yesterday
from cluster_source.database import MarketDataRepository

UTC = UTC


async def test_archive_day_moves_hot_to_parquet(
    share_config, share_repo: MarketDataRepository
) -> None:
    day = yesterday()
    share_repo.insert_ticks(
        [
            {
                "ticker": "SBER",
                "timestamp": day + timedelta(hours=2),
                "price": 100.0,
                "volume": 1,
                "direction": "BUY",
                "trade_id": "1",
            },
            {
                "ticker": "SBER",
                "timestamp": day + timedelta(hours=3),
                "price": 101.0,
                "volume": 2,
                "direction": "SELL",
                "trade_id": "2",
            },
        ]
    )
    archiver = DataArchiver(share_config, share_repo)
    ok = await archiver.archive_day(day)
    assert ok

    parquet = share_repo.read_parquet(day, day + timedelta(days=1))
    assert len(parquet) == 2
    assert share_repo.parquet_day_path(day.strftime("%Y-%m-%d")) is not None


async def test_archive_skips_empty_day(share_config, share_repo: MarketDataRepository) -> None:
    archiver = DataArchiver(share_config, share_repo)
    assert await archiver.archive_day(yesterday()) is True


async def test_run_once_purges_stale_hot_data(
    share_config, share_repo: MarketDataRepository
) -> None:
    stale = yesterday() - timedelta(days=10)
    share_repo.insert_ticks(
        [
            {
                "ticker": "SBER",
                "timestamp": stale,
                "price": 1.0,
                "volume": 1,
                "direction": "BUY",
                "trade_id": "1",
            },
            {
                "ticker": "SBER",
                "timestamp": yesterday() + timedelta(hours=1),
                "price": 2.0,
                "volume": 1,
                "direction": "SELL",
                "trade_id": "2",
            },
        ]
    )
    archiver = DataArchiver(share_config, share_repo)
    await archiver.run_once()
    remaining = share_repo.read_ticks(stale - timedelta(days=1), datetime.now(UTC))
    assert remaining["trade_id"].tolist() == ["2"]
