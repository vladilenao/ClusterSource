"""Загрузка конфигурации и доменная модель ClusterSource."""

from __future__ import annotations

import re
from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import date
from enum import StrEnum
from pathlib import Path

import yaml

#: Буквенные коды месяцев экспирации фьючерсов MOEX (F = январь … Z = декабрь).
FUTURES_MONTH_CODES = "FGHJKMNQUVXZ"


class Direction(StrEnum):
    """Направление агрессора сделки в хранилище."""

    BUY = "BUY"
    SELL = "SELL"
    #: Сделка без агрессора (аукцион, UNSPECIFIED у провайдера). Не входит
    #: ни в buy_volume, ни в sell_volume footprint.
    UNKNOWN = "UNKNOWN"


_BUY_VALUES = frozenset({"1", "BUY", "TRADE_DIRECTION_BUY"})
_SELL_VALUES = frozenset({"2", "SELL", "TRADE_DIRECTION_SELL"})


def parse_direction(raw: object) -> Direction:
    """Приводит направление из любого источника (SDK enum, REST, CSV) к :class:`Direction`."""
    value = str(raw if raw is not None else "").strip().upper()
    if value in _BUY_VALUES:
        return Direction.BUY
    if value in _SELL_VALUES:
        return Direction.SELL
    return Direction.UNKNOWN


class TradeSource(StrEnum):
    """Источник обезличенной сделки у T-Invest (SPEC-0002)."""

    EXCHANGE = "EXCHANGE"
    #: Внутренний рынок Т-Банка (дилерские сессии): в хранилище не попадает.
    DEALER = "DEALER"
    #: Источник не указан (UNSPECIFIED, нет поля или колонки).
    UNSPECIFIED = "UNSPECIFIED"
    #: Любое другое явное значение (например, ``TRADE_SOURCE_ALL``).
    OTHER = "OTHER"


_EXCHANGE_SOURCES = frozenset({"1", "EXCHANGE", "TRADE_SOURCE_EXCHANGE"})
_DEALER_SOURCES = frozenset({"2", "DEALER", "TRADE_SOURCE_DEALER"})
_UNSPECIFIED_SOURCES = frozenset({"", "0", "UNSPECIFIED", "TRADE_SOURCE_UNSPECIFIED"})


def parse_trade_source(raw: object) -> TradeSource:
    """Приводит источник сделки (SDK enum, REST, CSV архива) к :class:`TradeSource`."""
    value = str(raw if raw is not None else "").strip().upper()
    if value in _EXCHANGE_SOURCES:
        return TradeSource.EXCHANGE
    if value in _DEALER_SOURCES:
        return TradeSource.DEALER
    if value in _UNSPECIFIED_SOURCES:
        return TradeSource.UNSPECIFIED
    return TradeSource.OTHER


def is_exchange_trade(raw_source: object) -> bool:
    """Можно ли сохранить сделку с таким источником.

    SPEC-0002 G-1/G-3: сохраняются только биржевые сделки. Неуказанный
    источник принимается — биржевой фильтр стоит в самом запросе к
    источнику (G-2); любое явное небиржевое значение отбрасывается.
    """
    return parse_trade_source(raw_source) in (TradeSource.EXCHANGE, TradeSource.UNSPECIFIED)


class InstrumentType(StrEnum):
    SHARE = "share"
    FUTURES_CONTRACT = "futures_contract"
    CONTINUOUS_FUTURES = "continuous_futures"


@dataclass(frozen=True)
class Instrument:
    """Один инструмент для мониторинга, разобранный из config.yaml."""

    id: str
    type: InstrumentType
    tick_size: float
    lot_size: int

    @property
    def is_continuous(self) -> bool:
        return self.type == InstrumentType.CONTINUOUS_FUTURES

    @property
    def storage_name(self) -> str:
        """Каталог/ключ, под которым сохраняются данные этого инструмента.

        Непрерывные фьючерсы хранятся под общим непрерывным ключом, чтобы
        избежать дыр на границах контрактов, например ``Si_CONTINUOUS``.
        """
        if self.is_continuous:
            return f"{self.id}_CONTINUOUS"
        return self.id

    def is_contract_ticker(self, ticker: str) -> bool:
        """True, если ``ticker`` — контракт этого непрерывного фьючерса.

        Контракт MOEX — ``{id}{код месяца}{год}``, например ``SiZ6`` для ``Si``.
        Простой префикс недостаточен: он путает базовые активы с общим началом.
        """
        pattern = rf"{re.escape(self.id)}[{FUTURES_MONTH_CODES}]\d{{1,2}}"
        return re.fullmatch(pattern, ticker) is not None


@dataclass(frozen=True)
class FuturesMeta:
    """Минимальный дескриптор фьючерсного контракта из справочника провайдера."""

    ticker: str
    class_code: str
    expiration: date
    figi: str = ""


def nearest_contract(
    instrument: Instrument, futures: Iterable[FuturesMeta], day: date
) -> FuturesMeta | None:
    """Контракт непрерывного фьючерса, активный в ``day``.

    Активным считается контракт этого базового актива с ближайшей
    экспирацией в ``day`` или позже. ``None``, если такого контракта нет.
    Одно правило используется и для стрима, и для догрузки истории.
    """
    eligible = [
        m for m in futures if instrument.is_contract_ticker(m.ticker) and m.expiration >= day
    ]
    if not eligible:
        return None
    return min(eligible, key=lambda m: m.expiration)


@dataclass(frozen=True)
class BatchConfig:
    flush_interval_sec: float = 5.0
    batch_size: int = 1000


@dataclass(frozen=True)
class ArchiverConfig:
    run_hour_utc: int = 0
    run_minute_utc: int = 5


@dataclass(frozen=True)
class IssConfig:
    """Сбор ленты Мосбиржи из ISS (SPEC-0003)."""

    enabled: bool = True
    poll_interval_sec: float = 60.0


@dataclass(frozen=True)
class AppConfig:
    token: str
    data_dir: Path
    batch: BatchConfig = field(default_factory=BatchConfig)
    archiver: ArchiverConfig = field(default_factory=ArchiverConfig)
    iss: IssConfig = field(default_factory=IssConfig)
    #: SPEC-0010: на macOS не давать системе засыпать по простою, пока жив процесс.
    keep_awake: bool = True
    history_check_days: int = 14
    instruments: list[Instrument] = field(default_factory=list)

    @classmethod
    def from_yaml(cls, path: str | Path, token: str | None = None) -> AppConfig:
        """Разобрать файл ``config.yaml`` в :class:`AppConfig`.

        ``token`` может быть передан явно (например, из переменной окружения)
        и тогда переопределяет значение из YAML-файла.
        """
        path = Path(path)
        with path.open("r", encoding="utf-8") as fh:
            raw = yaml.safe_load(fh) or {}

        token_value = "your-token-here"
        if token is not None:
            token_value = token
        else:
            token_value = raw.get("TOKEN", raw.get("token", ""))

        instruments = [
            Instrument(
                id=item["id"],
                type=InstrumentType(item["type"]),
                tick_size=float(item.get("tick_size", 0.01)),
                lot_size=int(item.get("lot_size", 1)),
            )
            for item in raw.get("tickers", [])
        ]

        batch_raw = raw.get("batch", {})
        arch_raw = raw.get("archiver", {})
        iss_raw = raw.get("iss", {})

        return cls(
            token=token_value,
            data_dir=Path(raw.get("data_dir", "data")),
            batch=BatchConfig(
                flush_interval_sec=float(batch_raw.get("flush_interval_sec", 5.0)),
                batch_size=int(batch_raw.get("batch_size", 1000)),
            ),
            archiver=ArchiverConfig(
                run_hour_utc=int(arch_raw.get("run_hour_utc", 0)),
                run_minute_utc=int(arch_raw.get("run_minute_utc", 5)),
            ),
            iss=IssConfig(
                enabled=bool(iss_raw.get("enabled", True)),
                poll_interval_sec=float(iss_raw.get("poll_interval_sec", 60.0)),
            ),
            history_check_days=int(raw.get("history_check_days", 14)),
            keep_awake=bool(raw.get("keep_awake", True)),
            instruments=instruments,
        )

    @property
    def storage_root(self) -> Path:
        return self.data_dir / "history"
