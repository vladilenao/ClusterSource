"""Контракт непрерывного фьючерса на день — из сохранённого решения (SPEC-0014 G-1).

Отдельный модуль без зависимостей от ISS и календаря: им пользуются догрузка,
FINAL, сверка, читатель и стрим. Само решение принимает :mod:`cluster_source.roll`.
"""

from __future__ import annotations

import logging
from collections.abc import Mapping
from datetime import date

from cluster_source.config import FuturesMeta, Instrument, nearest_contract
from cluster_source.database import MarketDataRepository

logger = logging.getLogger(__name__)


def decided_ticker(repo: MarketDataRepository | None, day: date) -> str | None:
    """Тикер из решения на день или ``None``."""
    if repo is None:
        return None
    saved = repo.active_contract(day.isoformat())
    return None if saved is None else str(saved["ticker"])


def contract_of(
    instrument: Instrument,
    repo: MarketDataRepository | None,
    futures: Mapping[str, FuturesMeta],
    day: date,
) -> FuturesMeta | None:
    """Контракт непрерывного фьючерса на день: из решения, иначе по SPEC-0013 (G-1)."""
    ticker = decided_ticker(repo, day)
    if ticker is not None:
        meta = futures.get(ticker)
        if meta is not None:
            return meta
        logger.warning(
            "Контракт %s решения %s %s не найден в справочнике — по SPEC-0013",
            ticker,
            instrument.storage_name,
            day,
        )
    return nearest_contract(instrument, futures.values(), day)
