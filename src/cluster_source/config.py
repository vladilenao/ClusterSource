"""Загрузка конфигурации и доменная модель ClusterSource."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum
from pathlib import Path

import yaml


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


@dataclass(frozen=True)
class BatchConfig:
    flush_interval_sec: float = 5.0
    batch_size: int = 1000


@dataclass(frozen=True)
class ArchiverConfig:
    run_hour_utc: int = 0
    run_minute_utc: int = 5


@dataclass(frozen=True)
class AppConfig:
    token: str
    data_dir: Path
    batch: BatchConfig = field(default_factory=BatchConfig)
    archiver: ArchiverConfig = field(default_factory=ArchiverConfig)
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
            history_check_days=int(raw.get("history_check_days", 14)),
            instruments=instruments,
        )

    @property
    def storage_root(self) -> Path:
        return self.data_dir / "history"
