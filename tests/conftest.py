"""Общая фабрика фикстур для тестов: создаёт config и repo на tmp_path."""

from __future__ import annotations

from pathlib import Path

import pytest

from cluster_source.config import AppConfig, Instrument, InstrumentType
from cluster_source.database import MarketDataRepository

TOKEN = "test-token"


def make_config(tmp_path: Path, instruments: list[Instrument] | None = None) -> AppConfig:
    return AppConfig(
        token=TOKEN,
        data_dir=tmp_path / "data",
        instruments=instruments
        or [Instrument(id="SBER", type=InstrumentType.SHARE, tick_size=0.01, lot_size=1)],
    )


@pytest.fixture
def share_config(tmp_path: Path) -> AppConfig:
    return make_config(tmp_path)


@pytest.fixture
def share_repo(share_config: AppConfig) -> MarketDataRepository:
    repo = MarketDataRepository(share_config.storage_root, share_config.instruments[0])
    return repo
