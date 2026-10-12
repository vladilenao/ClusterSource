"""Config parsing tests."""

from __future__ import annotations

from pathlib import Path

from cluster_source.config import AppConfig, InstrumentType

SAMPLE = """
TOKEN: "secret-token"
data_dir: "custom_data"
batch:
  flush_interval_sec: 3
  batch_size: 500
tickers:
  - id: "SRZ6"
    type: "futures_contract"
    tick_size: 1.0
    lot_size: 1
  - id: "Si"
    type: "continuous_futures"
    tick_size: 1.0
    lot_size: 1
"""


def test_from_yaml_parses_instruments(tmp_path: Path) -> None:
    path = tmp_path / "config.yaml"
    path.write_text(SAMPLE, encoding="utf-8")
    cfg = AppConfig.from_yaml(path)

    assert cfg.token == "secret-token"
    assert cfg.data_dir == Path("custom_data")
    assert cfg.batch.flush_interval_sec == 3.0
    assert cfg.batch.batch_size == 500

    assert [i.type for i in cfg.instruments] == [
        InstrumentType.FUTURES_CONTRACT,
        InstrumentType.CONTINUOUS_FUTURES,
    ]
    assert cfg.instruments[1].is_continuous
    assert cfg.instruments[1].storage_name == "Si_CONTINUOUS"
    assert cfg.instruments[0].storage_name == "SRZ6"


def test_token_override(tmp_path: Path) -> None:
    path = tmp_path / "config.yaml"
    path.write_text('TOKEN: "ignored"\ntickers: []\n', encoding="utf-8")
    cfg = AppConfig.from_yaml(path, token="env-token")
    assert cfg.token == "env-token"


def test_is_contract_ticker_requires_month_and_year() -> None:
    from cluster_source.config import Instrument

    si = Instrument(id="Si", type=InstrumentType.CONTINUOUS_FUTURES, tick_size=1.0, lot_size=1)
    assert si.is_contract_ticker("SiZ6")
    assert si.is_contract_ticker("SiH27")
    assert not si.is_contract_ticker("SiZ")
    assert not si.is_contract_ticker("SILV")
    assert not si.is_contract_ticker("SRZ6")


def test_nearest_contract_filters_underlying_and_expiry() -> None:
    from datetime import date

    from cluster_source.config import FuturesMeta, Instrument, nearest_contract

    si = Instrument(id="Si", type=InstrumentType.CONTINUOUS_FUTURES, tick_size=1.0, lot_size=1)
    futures = [
        FuturesMeta("SiU6", "SPBFUT", date(2026, 9, 18)),
        FuturesMeta("SiZ6", "SPBFUT", date(2026, 12, 17)),
        FuturesMeta("SRU6", "SPBFUT", date(2026, 9, 1)),
    ]
    assert nearest_contract(si, futures, date(2026, 8, 1)).ticker == "SiU6"  # type: ignore[union-attr]
    assert nearest_contract(si, futures, date(2026, 9, 18)).ticker == "SiU6"  # type: ignore[union-attr]
    assert nearest_contract(si, futures, date(2026, 9, 19)).ticker == "SiZ6"  # type: ignore[union-attr]
    assert nearest_contract(si, futures, date(2027, 1, 1)) is None


def test_parse_direction() -> None:
    from cluster_source.config import Direction, parse_direction

    assert parse_direction(1) == Direction.BUY
    assert parse_direction("trade_direction_sell") == Direction.SELL
    assert parse_direction("2") == Direction.SELL
    assert parse_direction(0) == Direction.UNKNOWN
    assert parse_direction("") == Direction.UNKNOWN
    assert parse_direction(None) == Direction.UNKNOWN
