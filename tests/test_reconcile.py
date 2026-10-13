"""SPEC-0005: сверка стрима с лентой Мосбиржи и дозаливка."""

from __future__ import annotations

import logging
from datetime import UTC, datetime, timedelta
from typing import Any

import pandas as pd
import pytest

from cluster_source.config import AppConfig, Instrument, InstrumentType
from cluster_source.database import IssTapeRepository, MarketDataRepository
from cluster_source.reader import get_minute_verdicts, get_verified_until
from cluster_source.reconcile import (
    STATUS_GAP,
    STATUS_REPAIRED,
    STATUS_VERIFIED,
    Reconciler,
    reconcile_minute,
)

UTC = UTC
SBER = Instrument(id="SBER", type=InstrumentType.SHARE, tick_size=0.01, lot_size=1)
M0 = datetime(2026, 10, 13, 5, 0, tzinfo=UTC)  # минута сверки
NOW = M0 + timedelta(minutes=30)


def tick(sec: float, price: float = 100.0, vol: int = 1, side: str = "BUY") -> dict[str, object]:
    return {
        "ticker": "SBER",
        "timestamp": M0 + timedelta(seconds=sec),
        "price": price,
        "volume": vol,
        "direction": side,
        "trade_id": f"s{sec}",
    }


def iss_row(
    no: int, sec: int, price: float = 100.0, qty: int = 1, side: str = "BUY"
) -> dict[str, object]:
    return {
        "market": "shares",
        "feed_date": "2026-10-13",
        "secid": "SBER",
        "board": "TQBR",
        "tradeno": no,
        "timestamp": (M0 + timedelta(seconds=sec)).isoformat(timespec="microseconds"),
        "trade_date": "2026-10-13",
        "price": price,
        "quantity": qty,
        "direction": side,
        "period": "N",
        "session": "1",
        "open_position": None,
        "off_market": None,
    }


def other(no: int, sec: int) -> dict[str, object]:
    row = iss_row(no, sec)
    row["secid"] = "GAZP"
    return row


@pytest.fixture
def env(share_config: AppConfig) -> tuple[MarketDataRepository, IssTapeRepository, Reconciler]:
    repo = MarketDataRepository(share_config.storage_root, SBER)
    iss = IssTapeRepository(share_config.storage_root)
    iss.start_feed("shares", "1|x", "2026-10-13")
    # Границы ленты рынка задаёт другая бумага: начало до минуты сверки, конец — далеко после.
    iss.stage_rows([other(1, -60), other(999_999, 60 * 20)])
    rec = Reconciler(share_config, {"SBER": repo}, iss, lambda _i: "SBER", now=lambda: NOW)
    return repo, iss, rec


def stored(repo: MarketDataRepository) -> pd.DataFrame:
    return repo.read_ticks(M0, M0 + timedelta(minutes=1))


# --------------------------------------------------------------------------- #
def test_verified_assigns_tradeno(env: Any) -> None:
    """SPEC-0005 A-1: совпавшие секунды — VERIFIED, сделкам стрима проставлен TRADENO."""
    repo, iss, rec = env
    repo.insert_ticks([tick(1.25), tick(1.75, price=101.0), tick(5.5, side="SELL")])
    iss.stage_rows([iss_row(11, 1), iss_row(12, 1, price=101.0), iss_row(13, 5, side="SELL")])
    rec.reconcile_instrument(SBER)

    verdicts = get_minute_verdicts(repo, M0, M0 + timedelta(minutes=1))
    assert list(verdicts["status"]) == [STATUS_VERIFIED]
    df = stored(repo)
    assert sorted(df["exchange_trade_no"]) == [11, 12, 13]
    assert set(df["origin"]) == {"stream"}
    by_price = dict(zip(df["price"], df["exchange_trade_no"], strict=True))
    assert by_price[101.0] == 12  # номер — у «своей» сделки


def test_missing_seconds_repaired_from_iss(env: Any) -> None:
    """SPEC-0005 A-2 (G-2): секунды с потерями заменяются сделками ISS с origin=iss."""
    repo, iss, rec = env
    repo.insert_ticks([tick(1.5)])  # в 1-й секунде у ISS две сделки, 3-й секунды у нас нет
    iss.stage_rows([iss_row(11, 1), iss_row(12, 1, side="SELL"), iss_row(13, 3)])
    rec.reconcile_instrument(SBER)

    verdicts = get_minute_verdicts(repo, M0, M0 + timedelta(minutes=1))
    assert list(verdicts["status"]) == [STATUS_REPAIRED]
    assert int(verdicts["repaired"].iloc[0]) == 2
    df = stored(repo).sort_values("exchange_trade_no")
    assert list(df["exchange_trade_no"]) == [11, 12, 13]
    assert set(df["origin"]) == {"iss"}
    assert list(df["trade_id"]) == ["x11", "x12", "x13"]
    assert df["timestamp"].iloc[2] == M0 + timedelta(seconds=3)


def test_iss_empty_minute_is_gap(env: Any) -> None:
    """SPEC-0005 A-3: у ISS пусто при наших сделках — GAP, данные не тронуты."""
    repo, _iss, rec = env
    repo.insert_ticks([tick(1.5), tick(2.5)])
    rec.reconcile_instrument(SBER)
    verdicts = get_minute_verdicts(repo, M0, M0 + timedelta(minutes=1))
    assert list(verdicts["status"]) == [STATUS_GAP]
    assert verdicts["detail"].iloc[0] == "iss_empty"
    assert len(stored(repo)) == 2


def test_extra_trades_are_gap_not_deleted(env: Any) -> None:
    """SPEC-0005: у нас сделка, которой нет у ISS, — GAP, наши данные не удаляются."""
    repo, iss, rec = env
    repo.insert_ticks([tick(1.5), tick(1.6, price=99.0)])
    iss.stage_rows([iss_row(11, 1), iss_row(12, 2)])
    rec.reconcile_instrument(SBER)
    verdicts = get_minute_verdicts(repo, M0, M0 + timedelta(minutes=1))
    assert list(verdicts["status"]) == [STATUS_GAP]
    assert verdicts["detail"].iloc[0].startswith("extra_trades")
    assert len(stored(repo)) == 2 and set(stored(repo)["origin"]) == {"stream"}


def test_direction_mismatch(env: Any, caplog: pytest.LogCaptureFixture) -> None:
    """SPEC-0005 A-4: расходится только сторона — ISS_DIRECTION_MISMATCH, GAP."""
    repo, iss, rec = env
    repo.insert_ticks([tick(1.5, side="BUY"), tick(2.5, side="BUY")])
    iss.stage_rows([iss_row(11, 1, side="SELL"), iss_row(12, 2, side="SELL")])
    with caplog.at_level(logging.ERROR):
        rec.reconcile_instrument(SBER)
    assert list(get_minute_verdicts(repo, M0, M0 + timedelta(minutes=1))["status"]) == [STATUS_GAP]
    assert "ISS_DIRECTION_MISMATCH SBER" in caplog.text
    assert set(stored(repo)["direction"]) == {"BUY"}


def test_boundary_iss_edge_and_settle(share_config: AppConfig) -> None:
    """SPEC-0005 A-5: не сверяем минуты за последней сделкой ISS и моложе 16 минут."""
    repo = MarketDataRepository(share_config.storage_root, SBER)
    iss = IssTapeRepository(share_config.storage_root)
    iss.start_feed("shares", "1|x", "2026-10-13")
    iss.stage_rows([iss_row(1, 0), iss_row(2, 60 * 5 + 30)])  # лента до 05:05:30
    repo.insert_ticks([tick(1.0)])

    rec = Reconciler(share_config, {"SBER": repo}, iss, lambda _i: "SBER", now=lambda: NOW)
    rec.reconcile_instrument(SBER)
    assert get_verified_until(repo) == M0 + timedelta(minutes=5)  # минута 05:05 ещё не целиком

    early = Reconciler(
        share_config, {"SBER": repo}, iss, lambda _i: "SBER",
        now=lambda: M0 + timedelta(minutes=19),
    )  # fmt: skip
    iss.stage_rows([iss_row(3, 60 * 25)])
    early.reconcile_instrument(SBER)
    assert get_verified_until(repo) == M0 + timedelta(minutes=5)  # 05:03 < «сейчас − 16 мин»


def test_archived_day_not_touched(env: Any) -> None:
    """SPEC-0005 A-6 (G-3): день с Parquet-файлом сверка не меняет."""
    repo, iss, rec = env
    repo.insert_ticks([tick(1.5)])
    repo.write_parquet("2026-10-13", pd.DataFrame([tick(1.5)]))
    iss.stage_rows([iss_row(11, 1), iss_row(12, 3)])
    rec.reconcile_instrument(SBER)
    assert get_minute_verdicts(repo, M0, M0 + timedelta(minutes=1)).empty
    assert len(stored(repo)) == 1


def test_rerun_is_idempotent(env: Any) -> None:
    """SPEC-0005 A-8 (G-5): курсор и вердикт атомарны; повторный проход ничего не меняет."""
    repo, iss, rec = env
    repo.insert_ticks([tick(1.5)])
    iss.stage_rows([iss_row(11, 1), iss_row(12, 3)])
    first = rec.reconcile_instrument(SBER)
    before = stored(repo)
    second = rec.reconcile_instrument(SBER)
    assert first[STATUS_REPAIRED] == 1 and not second
    pd.testing.assert_frame_equal(before, stored(repo))


def test_blocked_instrument_waits(share_config: AppConfig, env: Any) -> None:
    """SPEC-0005: при backlog/write_failure сверка инструмента ждёт."""
    repo, iss, _rec = env
    rec = Reconciler(
        share_config, {"SBER": repo}, iss, lambda _i: "SBER",
        is_blocked=lambda _n: True, now=lambda: NOW,
    )  # fmt: skip
    rec.reconcile_instrument(SBER)
    assert get_verified_until(repo) is None


def test_reconcile_minute_empty_is_none() -> None:
    assert reconcile_minute("SBER", [], []) is None


def test_legacy_hot_store_migrated(share_config: AppConfig) -> None:
    """SPEC-0005: старая база без колонок origin/exchange_trade_no мигрирует при открытии."""
    import sqlite3

    path = share_config.storage_root / "_sqlite" / "SBER.sqlite3"
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    conn.execute(
        "CREATE TABLE ticks_hot_store (ticker TEXT NOT NULL, timestamp DATETIME NOT NULL,"
        " price REAL NOT NULL, volume INTEGER NOT NULL, direction TEXT NOT NULL,"
        " trade_id TEXT NOT NULL)"
    )
    conn.execute(
        "INSERT INTO ticks_hot_store VALUES ('SBER', '2026-10-13T05:00:01.000000+00:00', 1, 1,"
        " 'BUY', 's1')"
    )
    conn.commit()
    conn.close()
    repo = MarketDataRepository(share_config.storage_root, SBER)
    df = stored(repo)
    assert list(df["origin"]) == ["stream"] and df["exchange_trade_no"].isna().all()
