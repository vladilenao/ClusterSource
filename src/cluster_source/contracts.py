"""Контракт непрерывного фьючерса на день — из сохранённого решения (SPEC-0014 G-1).

Отдельный модуль без зависимостей от ISS и календаря: им пользуются догрузка,
FINAL, сверка, читатель и стрим. Само решение принимает :mod:`cluster_source.roll`.
"""

from __future__ import annotations

import logging
from collections.abc import Iterable, Mapping
from dataclasses import replace
from datetime import date, timedelta

from cluster_source.config import (
    AppConfig,
    FuturesMeta,
    Instrument,
    InstrumentType,
    nearest_contract,
)
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


# ---------------------------------------------------------------------- #
# Хранилища контрактов (SPEC-0014 G-3)
# ---------------------------------------------------------------------- #
def contract_storages(
    config: AppConfig,
    futures: Iterable[FuturesMeta],
    first_days: Mapping[str, date],
    today: date,
) -> list[Instrument]:
    """Хранилища контрактов, окно которых пересекает [первый день актива, сегодня].

    Окно контракта — дни D, когда он среди N ближайших по последнему дню
    торгов (N — :meth:`AppConfig.contracts_of`): от дня после истечения
    контракта, стоящего на N раньше, до своего последнего дня торгов.
    ``first_days`` — первый сохранённый день непрерывного инструмента.
    Явный ``futures_contract`` с тем же тикером — ошибка конфига.
    """
    explicit = {i.id for i in config.instruments}
    metas = list(futures)
    found: list[Instrument] = []
    for inst in config.instruments:
        count = config.contracts_of(inst)
        if not inst.is_continuous or count == 0:
            continue
        own = sorted(
            (m for m in metas if inst.is_contract_ticker(m.ticker)), key=lambda m: m.active_until
        )
        first = first_days.get(inst.storage_name, today)
        for index, meta in enumerate(own):
            start = first
            if index >= count:
                start = max(first, own[index - count].active_until + timedelta(days=1))
            end = meta.active_until
            if end < first or start > today or start > end:
                continue
            if meta.ticker in explicit:
                raise ValueError(
                    f"{meta.ticker}: явный futures_contract совпадает с хранилищем контракта "
                    f"{inst.id} (SPEC-0014) — уберите его из config.yaml"
                )
            found.append(
                Instrument(
                    id=meta.ticker,
                    type=InstrumentType.FUTURES_CONTRACT,
                    tick_size=inst.tick_size,
                    lot_size=inst.lot_size,
                    underlying=inst.id,
                    window=(start, end),
                )
            )
    return found


def with_storages(config: AppConfig, storages: Iterable[Instrument]) -> AppConfig:
    """Конфиг для сбора и обслуживания: инструменты config.yaml и хранилища контрактов."""
    base = [i for i in config.instruments if not i.is_contract_storage]
    return replace(config, instruments=[*base, *storages])
