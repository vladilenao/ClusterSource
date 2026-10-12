"""Тесты автозагрузки: разбор CSV, обнаружение пропущенных дней, конвертация GetLastTrades."""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta

import pandas as pd

from cluster_source.autoload import (
    BackfillResult,
    _history_rows,
    _parse_history_csv,
    _persist_day,
    _pick_share,
    _settle_refresh_mark,
    _trades_to_rows,
    archive_key_candidates,
    current_figi,
    missing_days,
    refresh_days,
)
from cluster_source.config import AppConfig, FuturesMeta, Instrument, InstrumentType
from cluster_source.database import MarketDataRepository

UTC = UTC

CSV = """tradeId,time,price,quantity,direction
1,2026-08-20 10:00:00,100.5,3,BUY
2,2026-08-20 10:00:01,100.6,2,SELL
"""

CSV_CAPS = """TradeID,Time,Price,Volume,Direction
1,2026-08-20 10:00:00,50.0,1,sell
"""

TRADE_ARCHIVE_CSV = """TRADE_TS,TICKER_CC,DIRECTION,PRICE,QUANTITY,TRADE_SOURCE,INSTRUMENT_UID
2026-09-04T03:59:35.341871Z,SBER_TQBR,BUY,279.6,800,EXCHANGE,e6123145-9665
2026-09-04T03:59:36.000000Z,SBER_TQBR,SELL,279.8,6,EXCHANGE,e6123145-9665
2026-09-04T03:59:36.100000Z,SBER_TQBR,BUY,279.7,12,EXCHANGE,e6123145-9665
"""

SRZ6 = Instrument(id="SRZ6", type=InstrumentType.FUTURES_CONTRACT, tick_size=1, lot_size=1)
SI = Instrument(id="Si", type=InstrumentType.CONTINUOUS_FUTURES, tick_size=1, lot_size=1)
SBER = Instrument(id="SBER", type=InstrumentType.SHARE, tick_size=0.01, lot_size=1)


def test_parse_history_csv() -> None:
    df = _parse_history_csv(CSV.encode("utf-8"))
    assert len(df) == 2
    assert df["price"].tolist() == [100.5, 100.6]
    assert df["trade_id"].tolist() == ["1", "2"]
    assert df["timestamp"].dt.tz is not None


def test_parse_history_csv_bom_and_direction_case() -> None:
    df = _parse_history_csv(b"\xef\xbb\xbf" + CSV_CAPS.encode("utf-8"))
    assert len(df) == 1
    assert df["direction"].iloc[0].upper() == "SELL"
    assert df["price"].iloc[0] == 50.0


def test_parse_trade_archive_csv() -> None:
    df = _parse_history_csv(TRADE_ARCHIVE_CSV.encode("utf-8"))
    assert len(df) == 3
    assert df["price"].tolist() == [279.6, 279.8, 279.7]
    assert df["volume"].tolist() == [800, 6, 12]
    assert df["direction"].tolist() == ["BUY", "SELL", "BUY"]
    assert df["timestamp"].iloc[0].isoformat() == "2026-09-04T03:59:35.341871+00:00"


def test_history_rows_from_trade_archive() -> None:
    df = _parse_history_csv(TRADE_ARCHIVE_CSV.encode("utf-8"))
    rows = _history_rows(df, SBER)
    assert len(rows) == 3
    assert rows[0]["ticker"] == "SBER"
    assert rows[0]["price"] == 279.6
    assert rows[0]["volume"] == 800
    assert rows[0]["timestamp"].tzinfo == UTC
    # У строк архива нет trade id -> синтетический нечисловой id (пропускает проверку пропусков)
    assert rows[0]["trade_id"].startswith("s")


def test_archive_key_candidates() -> None:
    futures = {
        "SiU5": FuturesMeta("SiU5", "SPBFUT", date(2026, 6, 18)),
        "SiU6": FuturesMeta("SiU6", "SPBFUT", date(2026, 9, 18)),
        "SRZ6": FuturesMeta("SRZ6", "SPBFUT", date(2026, 9, 18)),
    }
    day_before_u6 = datetime(2026, 8, 20, tzinfo=UTC)
    day_before_any = datetime(2026, 1, 10, tzinfo=UTC)
    assert archive_key_candidates(SI, day_before_u6, futures) == ["SiU6_SPBFUT"]
    assert archive_key_candidates(SI, day_before_any, futures) == ["SiU5_SPBFUT"]
    assert archive_key_candidates(SRZ6, day_before_u6, futures) == ["SRZ6_SPBFUT"]
    assert archive_key_candidates(SRZ6, day_before_u6, {}) == ["SRZ6_SPBFUT"]
    assert archive_key_candidates(SBER, day_before_u6, futures) == [
        "SBER_TQBR",
        "SBER_SPBXM",
    ]


def test_continuous_key_ignores_other_underlyings() -> None:
    """KI-04: контракт другого базового актива с более ранней экспирацией не выбирается."""
    futures = {
        "SRU6": FuturesMeta("SRU6", "SPBFUT", date(2026, 9, 17)),
        "SiU6": FuturesMeta("SiU6", "SPBFUT", date(2026, 9, 18)),
        "SiZ6": FuturesMeta("SiZ6", "SPBFUT", date(2026, 12, 17)),
    }
    assert archive_key_candidates(SI, datetime(2026, 9, 1, tzinfo=UTC), futures) == ["SiU6_SPBFUT"]
    assert archive_key_candidates(SI, datetime(2026, 9, 19, tzinfo=UTC), futures) == ["SiZ6_SPBFUT"]
    # Нет контракта, активного в этот день, — ключей нет (а не чужой контракт).
    assert archive_key_candidates(SI, datetime(2027, 1, 1, tzinfo=UTC), futures) == []


def test_current_figi_for_today() -> None:
    futures = {
        "SiZ6": FuturesMeta("SiZ6", "SPBFUT", date(2026, 12, 17), figi="FUT_SIZ6"),
        "SRZ6": FuturesMeta("SRZ6", "SPBFUT", date(2026, 12, 16), figi="FUT_SRZ6"),
    }
    today = date(2026, 10, 12)
    assert current_figi(SI, {}, futures, today) == "FUT_SIZ6"
    assert current_figi(SRZ6, {}, futures, today) == "FUT_SRZ6"
    assert current_figi(SBER, {"SBER": "BBG004730N88"}, futures, today) == "BBG004730N88"
    assert current_figi(SBER, {"SBER": None}, futures, today) is None


def test_pick_share_exact_ticker_and_class_priority() -> None:
    """KI-12: нет точного совпадения — None, а не первый попавшийся инструмент."""
    items = [
        {"ticker": "SBERP", "classCode": "TQBR", "figi": "P"},
        {"ticker": "T", "classCode": "SPBXM", "figi": "ATT"},
        {"ticker": "T", "classCode": "TQBR", "figi": "TCS"},
    ]
    assert _pick_share(items, "T")["figi"] == "TCS"
    assert _pick_share(items, "SBER") is None


def test_history_rows_direction_normalized() -> None:
    """KI-10: без колонки направления сделка получает UNKNOWN, а не BUY."""
    csv_no_dir = "TRADE_TS,PRICE,QUANTITY\n2026-09-04T10:00:00Z,100.0,1\n"
    rows = _history_rows(_parse_history_csv(csv_no_dir.encode()), SBER)
    assert rows[0]["direction"] == "UNKNOWN"


def test_trades_to_rows_rest_shape() -> None:
    instr = Instrument(id="SBER", type=InstrumentType.SHARE, tick_size=0.01, lot_size=1)
    raw = [
        {
            "figi": "BBG004730N88",
            "direction": "TRADE_DIRECTION_BUY",
            "price": {"units": 312, "nano": 500000000},
            "quantity": 5,
            "time": "2026-09-05T10:00:01.123Z",
            "trade_id": "7",
        },
        {
            "figi": "BBG004730N88",
            "direction": "2",
            "price": 313.0,
            "volume": 2,
            "timestamp": 1757041202.0,
        },
    ]
    rows = _trades_to_rows(raw, instr)
    assert len(rows) == 2
    assert rows[0]["ticker"] == "SBER"
    assert rows[0]["price"] == 312.5
    assert rows[0]["direction"] == "BUY"
    assert rows[0]["volume"] == 5
    assert rows[0]["trade_id"] == "7"
    assert rows[0]["timestamp"].tzinfo == UTC
    assert rows[1]["direction"] == "SELL"
    assert rows[1]["price"] == 313.0
    unspecified = _trades_to_rows(
        [
            {
                "time": "2026-09-05T10:00:00Z",
                "price": 1.0,
                "direction": "TRADE_DIRECTION_UNSPECIFIED",
            }
        ],
        instr,
    )
    assert unspecified[0]["direction"] == "UNKNOWN"


def test_missing_days_detects_only_absent(share_config: AppConfig, tmp_path) -> None:
    instr = Instrument(id="SBER", type=InstrumentType.SHARE, tick_size=0.01, lot_size=1)
    cfg = AppConfig(token="t", data_dir=share_config.data_dir, instruments=[instr])
    repo = MarketDataRepository(cfg.storage_root, instr)

    # Добавляем один горячий день + один холодный Parquet-день в далёком прошлом
    today = datetime.now(UTC).replace(hour=10, minute=0, second=0, microsecond=0)
    repo.insert_ticks(
        [
            {
                "ticker": "SBER",
                "timestamp": today,
                "price": 1.0,
                "volume": 1,
                "direction": "BUY",
                "trade_id": "1",
            },
            {
                "ticker": "SBER",
                "timestamp": today - timedelta(days=3),
                "price": 1.0,
                "volume": 1,
                "direction": "SELL",
                "trade_id": "2",
            },
        ]
    )
    old = today - timedelta(days=20)
    df = pd.DataFrame(
        {
            "ticker": ["SBER"],
            "timestamp": [old],
            "price": [1.0],
            "volume": [1],
            "direction": ["BUY"],
            "trade_id": ["x"],
        }
    )
    repo.write_parquet(old.strftime("%Y-%m-%d"), df)

    missing = missing_days(cfg, repo)
    missing_str = {d.strftime("%Y-%m-%d") for d in missing}
    # Присутствующие дни не должны попадать в список пропущенных
    assert today.strftime("%Y-%m-%d") not in missing_str
    assert (today - timedelta(days=3)).strftime("%Y-%m-%d") not in missing_str
    # Внутри 14-дневного окна проверки остаются только отсутствующие дни
    assert len(missing) == cfg.history_check_days - 2


def _row(ts: datetime, price: float, tid: str) -> dict[str, object]:
    return {
        "ticker": "SBER",
        "timestamp": ts,
        "price": price,
        "volume": 1,
        "direction": "BUY",
        "trade_id": tid,
    }


def test_refresh_days_window_and_today(share_config: AppConfig) -> None:
    """KI-03/KI-09: отмеченные прошедшие дни перекачиваются, сегодняшний ждёт архива."""
    repo = MarketDataRepository(share_config.storage_root, share_config.instruments[0])
    today = datetime.now(UTC).replace(hour=0, minute=0, second=0, microsecond=0)
    fmt = "%Y-%m-%d"
    repo.mark_days_for_refresh([today.strftime(fmt)], "stream_gap")
    repo.mark_days_for_refresh([(today - timedelta(days=2)).strftime(fmt)], "compromised")
    stale = (today - timedelta(days=share_config.history_check_days + 3)).strftime(fmt)
    repo.mark_days_for_refresh([stale], "compromised")

    days = refresh_days(share_config, repo)
    assert [d.strftime(fmt) for d in days] == [(today - timedelta(days=2)).strftime(fmt)]
    assert stale not in repo.days_to_refresh()  # вышла за окно — снята
    assert today.strftime(fmt) in repo.days_to_refresh()


async def test_persist_day_replace_overwrites_hot_and_parquet(share_config: AppConfig) -> None:
    repo = MarketDataRepository(share_config.storage_root, share_config.instruments[0])
    day = datetime.now(UTC).replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=2)
    stream_rows = [_row(day + timedelta(hours=10), 100.0, "s1")]
    repo.insert_ticks(stream_rows)
    repo.write_parquet(day.strftime("%Y-%m-%d"), pd.DataFrame(stream_rows))

    archive_rows = [
        _row(day + timedelta(hours=9), 99.0, "a0"),
        _row(day + timedelta(hours=10), 100.0, "a1"),
    ]
    await _persist_day(repo, day, archive_rows, replace=True)

    hot = repo.hot_rows_for_day(day)
    cold = repo.read_parquet(day, day + timedelta(days=1))
    assert sorted(hot["trade_id"]) == ["a0", "a1"]
    assert sorted(cold["trade_id"]) == ["a0", "a1"]


def test_settle_refresh_mark(share_config: AppConfig) -> None:
    repo = MarketDataRepository(share_config.storage_root, share_config.instruments[0])
    day = datetime.now(UTC).replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=1)
    day_str = day.strftime("%Y-%m-%d")

    repo.mark_days_for_refresh([day_str], "stream_gap")
    _settle_refresh_mark(repo, day, BackfillResult.NO_DATA)  # данных нет — торгов не было
    assert day_str not in repo.days_to_refresh()

    repo.insert_ticks([_row(day + timedelta(hours=10), 1.0, "s1")])
    repo.mark_days_for_refresh([day_str], "stream_gap")
    _settle_refresh_mark(repo, day, BackfillResult.NO_DATA)  # архив ещё не опубликован
    assert day_str in repo.days_to_refresh()
    _settle_refresh_mark(repo, day, BackfillResult.FAILED)
    assert day_str in repo.days_to_refresh()
    _settle_refresh_mark(repo, day, BackfillResult.WRITTEN)
    assert day_str not in repo.days_to_refresh()


def test_parse_history_csv_keeps_rows_without_fraction() -> None:
    csv_mixed = (
        "TRADE_TS,TICKER_CC,DIRECTION,PRICE,QUANTITY,TRADE_SOURCE,INSTRUMENT_UID\n"
        "2026-09-04T07:00:00.250000Z,SBER_TQBR,BUY,279.6,1,EXCHANGE,u\n"
        "2026-09-04T07:00:01Z,SBER_TQBR,SELL,279.7,2,EXCHANGE,u\n"
    )
    df = _parse_history_csv(csv_mixed.encode())
    assert len(df) == 2
