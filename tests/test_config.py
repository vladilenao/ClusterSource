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
