"""Тесты автозагрузки: разбор CSV, обнаружение пропущенных дней, конвертация GetLastTrades."""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta

import pandas as pd

from cluster_source.autoload import (
    _archive_key_candidates,
    _FuturesMeta,
    _history_rows,
    _parse_history_csv,
    _trades_to_rows,
    missing_days,
)
from cluster_source.config import AppConfig, Instrument, InstrumentType
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
        "SiU5": _FuturesMeta("SiU5", "SPBFUT", date(2026, 6, 18)),
        "SiU6": _FuturesMeta("SiU6", "SPBFUT", date(2026, 9, 18)),
        "SRZ6": _FuturesMeta("SRZ6", "SPBFUT", date(2026, 9, 18)),
    }
    day_before_u6 = datetime(2026, 8, 20, tzinfo=UTC)
    day_before_any = datetime(2026, 1, 10, tzinfo=UTC)
    assert _archive_key_candidates(SI, day_before_u6, futures) == ["SiU6_SPBFUT"]
    assert _archive_key_candidates(SI, day_before_any, futures) == ["SiU5_SPBFUT"]
    assert _archive_key_candidates(SRZ6, day_before_u6, futures) == ["SRZ6_SPBFUT"]
    assert _archive_key_candidates(SRZ6, day_before_u6, {}) == ["SRZ6_SPBFUT"]
    assert _archive_key_candidates(SBER, day_before_u6, futures) == [
        "SBER_TQBR",
        "SBER_SPBXM",
    ]


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
