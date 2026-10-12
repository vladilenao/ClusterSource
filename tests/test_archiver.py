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


def _row(ts: datetime, tid: str) -> dict:
    return {
        "ticker": "SBER",
        "timestamp": ts,
        "price": 1.0,
        "volume": 1,
        "direction": "BUY",
        "trade_id": tid,
    }


async def test_run_once_archives_every_unarchived_past_day(
    share_config, share_repo: MarketDataRepository
) -> None:
    """KI-02: архивируется не только «вчера», и ничто не удаляется без Parquet."""
    today = yesterday() + timedelta(days=1)
    days = [today - timedelta(days=n) for n in (3, 9)]
    share_repo.insert_ticks([_row(d + timedelta(hours=10), d.strftime("%d")) for d in days])
    share_repo.insert_ticks([_row(datetime.now(UTC), "today")])

    await DataArchiver(share_config, share_repo).run_once()

    archived = share_repo.parquet_days_available()
    assert {d.strftime("%Y-%m-%d") for d in days} <= archived
    assert today.strftime("%Y-%m-%d") not in archived
    assert share_repo.hot_days_available() == {
        days[0].strftime("%Y-%m-%d"),
        today.strftime("%Y-%m-%d"),
    }


async def test_purge_skipped_when_archive_fails(
    share_config, share_repo: MarketDataRepository, monkeypatch
) -> None:
    from cluster_source.database import DataCorruptionError

    stale = yesterday() - timedelta(days=10)
    share_repo.insert_ticks([_row(stale + timedelta(hours=10), "1")])

    def broken(*_args: object) -> None:
        raise DataCorruptionError("disk full")

    monkeypatch.setattr(share_repo, "write_parquet", broken)
    await DataArchiver(share_config, share_repo).run_once()
    assert share_repo.count_hot() == 1


def test_seconds_until_next_run() -> None:
    from cluster_source.archiver import seconds_until_next_run
    from cluster_source.config import ArchiverConfig

    sched = ArchiverConfig(run_hour_utc=0, run_minute_utc=5)
    assert seconds_until_next_run(datetime(2026, 10, 12, 0, 0, tzinfo=UTC), sched) == 300
    assert seconds_until_next_run(datetime(2026, 10, 12, 0, 5, tzinfo=UTC), sched) == 86400
    assert seconds_until_next_run(datetime(2026, 10, 12, 23, 0, tzinfo=UTC), sched) == 3900
