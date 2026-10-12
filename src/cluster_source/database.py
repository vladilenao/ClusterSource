"""Слой доступа к данным: гибридное хранилище SQLite (горячее) + Parquet (холодное).

Вся персистентность инкапсулирована в :class:`MarketDataRepository`. Бизнес-
логика никогда не выполняет сырые SQL-запросы напрямую.
"""

from __future__ import annotations

import logging
import sqlite3
from collections.abc import Iterable, Iterator, Sequence
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

import pandas as pd

from cluster_source.config import Instrument

logger = logging.getLogger(__name__)

HOT_RETENTION_DAYS = 7


class DataCorruptionError(RuntimeError):
    """Возникает, когда шаг архивации создал некорректный или пустой Parquet-файл."""


class MarketDataRepository:
    """Владеет горячим SQLite-хранилищем и холодным Parquet-хранилищем.

    Предназначен для внедрения (Dependency Injection) в коллектор и архиватор,
    а не для глобального импорта.
    """

    def __init__(self, repo_path: Path, instrument: Instrument) -> None:
        self.instrument = instrument
        self._storage_dir = repo_path / instrument.storage_name
        self._memory_dir = repo_path / "_sqlite"
        self._memory_dir.mkdir(parents=True, exist_ok=True)
        self._db_path = self._memory_dir / f"{instrument.storage_name}.sqlite3"

    # ------------------------------------------------------------------ #
    # Управление подключениями
    # ------------------------------------------------------------------ #
    @contextmanager
    def session(self) -> Iterator[sqlite3.Connection]:
        """Контекстный менеджер, отдающий SQLite-подключение в режиме WAL.

        Режим WAL критичен: торговый робот читает тики из той же базы, пока
        коллектор непрерывно пишет в неё.
        """
        conn = sqlite3.connect(self._db_path, timeout=30)
        try:
            conn.execute("PRAGMA journal_mode=WAL;")
            conn.execute("PRAGMA synchronous=NORMAL;")
            conn.execute("PRAGMA busy_timeout=15000;")
            self._ensure_schema(conn)
            yield conn
        finally:
            conn.close()

    @staticmethod
    def _ensure_schema(conn: sqlite3.Connection) -> None:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS ticks_hot_store (
                ticker     TEXT NOT NULL,
                timestamp  DATETIME NOT NULL,
                price      REAL NOT NULL,
                volume     INTEGER NOT NULL,
                direction  TEXT NOT NULL,
                trade_id   TEXT NOT NULL
            )
            """
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_ticks_ticker_ts ON ticks_hot_store (ticker, timestamp)"
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS days_to_refresh (
                day        TEXT PRIMARY KEY,
                reason     TEXT NOT NULL,
                marked_at  TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS meta (
                key    TEXT PRIMARY KEY,
                value  TEXT NOT NULL
            )
            """
        )

    # ------------------------------------------------------------------ #
    # Запись в горячее хранилище (SQLite) — батчами
    # ------------------------------------------------------------------ #
    def insert_ticks(self, rows: Sequence[dict[str, object]]) -> None:
        """Массовая вставка строк в рамках одной явной транзакции.

        ``rows`` — словари с ключами: ticker, timestamp, price, volume,
        direction, trade_id.
        """
        if not rows:
            return
        with self.session() as conn:
            conn.execute("BEGIN TRANSACTION;")
            try:
                self._insert_rows(conn, rows)
                conn.commit()
            except Exception:
                conn.rollback()
                raise

    def replace_hot_day(self, day: str, rows: Sequence[dict[str, object]]) -> None:
        """Атомарно заменяет все строки дня ``day`` (UTC) этого инструмента на ``rows``.

        Удаление и вставка выполняются в одной транзакции, поэтому читатель
        никогда не видит день пустым или задвоенным.
        """
        with self.session() as conn:
            conn.execute("BEGIN TRANSACTION;")
            try:
                conn.execute(
                    "DELETE FROM ticks_hot_store WHERE ticker = ? AND substr(timestamp, 1, 10) = ?",
                    (self.instrument.storage_name, day),
                )
                self._insert_rows(conn, rows)
                conn.commit()
            except Exception:
                conn.rollback()
                raise

    def _insert_rows(self, conn: sqlite3.Connection, rows: Sequence[dict[str, object]]) -> None:
        conn.executemany(
            """
            INSERT INTO ticks_hot_store
                (ticker, timestamp, price, volume, direction, trade_id)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    r["ticker"],
                    self._iso(r["timestamp"]),
                    self._as_float(r["price"]),
                    self._as_int(r["volume"]),
                    r["direction"],
                    str(r["trade_id"]),
                )
                for r in rows
            ],
        )

    @staticmethod
    def _iso(value: object) -> str:
        if isinstance(value, datetime):
            if value.tzinfo is None:
                value = value.replace(tzinfo=UTC)
            # Всегда с микросекундами: единый формат строк в SQLite.
            return value.astimezone(UTC).isoformat(timespec="microseconds")
        return str(value)

    @staticmethod
    def _as_float(value: object) -> float:
        return float(cast(Any, value))

    @staticmethod
    def _as_int(value: object) -> int:
        return int(cast(Any, value))

    # ------------------------------------------------------------------ #
    # Чтение горячего хранилища (SQLite)
    # ------------------------------------------------------------------ #
    def read_ticks(self, start: datetime, end: datetime, ticker: str | None = None) -> pd.DataFrame:
        """Читает тики из горячего хранилища за [start, end).

        Временные метки нормализуются в UTC.
        """
        ticker = ticker or self.instrument.storage_name
        with self.session() as conn:
            df = pd.read_sql_query(
                """
                SELECT ticker, timestamp, price, volume, direction, trade_id
                FROM ticks_hot_store
                WHERE ticker = ?
                  AND timestamp >= ? AND timestamp < ?
                """,
                conn,
                params=(ticker, self._iso(start), self._iso(end)),
            )
        if not df.empty:
            # Строки, записанные до унификации формата, бывают без дробной части.
            df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, format="ISO8601")
        return df

    def hot_rows_for_day(self, day: datetime) -> pd.DataFrame:
        """Возвращает все строки горячего хранилища за указанный календарный день (UTC)."""
        day = self._utc_midnight(day)
        return self.read_ticks(day, day + pd.Timedelta(days=1))

    def purge_hot_before(self, cutoff: datetime) -> int:
        """Удаляет строки горячего хранилища строго старше ``cutoff`` (UTC)."""
        with self.session() as conn:
            conn.execute("BEGIN TRANSACTION;")
            try:
                cur = conn.execute(
                    "DELETE FROM ticks_hot_store WHERE timestamp < ?",
                    (self._iso(cutoff),),
                )
                rowcount = cur.rowcount
                conn.commit()
            except Exception:
                conn.rollback()
                raise
        logger.info("Из горячего хранилища удалено %d устаревших строк", rowcount)
        return rowcount

    def count_hot(self) -> int:
        with self.session() as conn:
            cur = conn.execute("SELECT COUNT(*) FROM ticks_hot_store")
            row = cur.fetchone()
            return 0 if row is None else int(row[0])

    def hot_days_available(self, ticker: str | None = None) -> set[str]:
        """Возвращает набор строк дат (UTC), присутствующих в горячем хранилище."""
        ticker = ticker or self.instrument.storage_name
        with self.session() as conn:
            cur = conn.execute(
                "SELECT DISTINCT substr(timestamp, 1, 10) FROM ticks_hot_store WHERE ticker = ?",
                (ticker,),
            )
            return {row[0] for row in cur.fetchall()}

    # ------------------------------------------------------------------ #
    # Отметки «день требует перекачки из официального архива»
    # ------------------------------------------------------------------ #
    def mark_days_for_refresh(
        self, days: Iterable[str], reason: str, *, override: bool = False
    ) -> None:
        """Отмечает дни (``YYYY-MM-DD``, UTC) как неполные.

        Повторная отметка не меняет причину, если не задан ``override``.
        """
        marked_at = datetime.now(UTC).isoformat()
        params = [(day, reason, marked_at) for day in days]
        if not params:
            return
        verb = "INSERT OR REPLACE" if override else "INSERT OR IGNORE"
        with self.session() as conn:
            conn.executemany(
                f"{verb} INTO days_to_refresh (day, reason, marked_at) VALUES (?, ?, ?)",
                params,
            )
            conn.commit()

    def days_to_refresh(self) -> dict[str, str]:
        """Возвращает отмеченные дни: ``{YYYY-MM-DD: причина}``."""
        with self.session() as conn:
            cur = conn.execute("SELECT day, reason FROM days_to_refresh")
            return {str(day): str(reason) for day, reason in cur.fetchall()}

    def clear_refresh_mark(self, day: str) -> None:
        with self.session() as conn:
            conn.execute("DELETE FROM days_to_refresh WHERE day = ?", (day,))
            conn.commit()

    # ------------------------------------------------------------------ #
    # Служебные флаги (однократные операции обслуживания)
    # ------------------------------------------------------------------ #
    def get_meta(self, key: str) -> str | None:
        with self.session() as conn:
            row = conn.execute("SELECT value FROM meta WHERE key = ?", (key,)).fetchone()
            return None if row is None else str(row[0])

    def set_meta(self, key: str, value: str) -> None:
        with self.session() as conn:
            conn.execute("INSERT OR REPLACE INTO meta (key, value) VALUES (?, ?)", (key, value))
            conn.commit()

    # ------------------------------------------------------------------ #
    # Запись в холодное хранилище (Parquet)
    # ------------------------------------------------------------------ #
    def write_parquet(self, day: str, df: pd.DataFrame) -> Path:
        """Сохраняет полный день тиков в ``{storage}/{{YYYY-MM-DD}}.parquet``.

        Возвращает путь к файлу. Выбрасывает :class:`DataCorruptionError`,
        если файл не создан или пуст, чтобы вызывающий код мог проверять
        результат до очистки горячего хранилища.
        """
        if df.empty:
            raise DataCorruptionError(
                f"Нет строк для архивации: {self.instrument.storage_name} {day}"
            )

        df = df.copy()
        if not df.empty and "timestamp" in df.columns:
            df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)

        self._storage_dir.mkdir(parents=True, exist_ok=True)
        day_str = pd.Timestamp(day).strftime("%Y-%m-%d")
        path = self._storage_dir / f"{day_str}.parquet"
        df.to_parquet(path, compression="snappy", index=False)

        if not path.exists() or path.stat().st_size == 0:
            raise DataCorruptionError(
                f"Некорректный файл архива для {self.instrument.storage_name} {day}: {path}"
            )
        logger.info("Записан Parquet %s (%d строк)", path, len(df))
        return path

    # ------------------------------------------------------------------ #
    # Чтение холодного хранилища (Parquet)
    # ------------------------------------------------------------------ #
    def parquet_days_available(self) -> set[str]:
        if not self._storage_dir.exists():
            return set()
        return {p.stem for p in self._storage_dir.glob("*.parquet")}

    def read_parquet(self, start: datetime, end: datetime) -> pd.DataFrame:
        """Читает тики холодного хранилища с ``timestamp`` в [start, end) (UTC).

        Открываются только файлы дней, пересекающих интервал; строки
        фильтруются по точному времени.
        """
        start, end = self._as_utc(start), self._as_utc(end)
        first_day = self._utc_midnight(start)
        frames: list[pd.DataFrame] = []
        for path in sorted(self._storage_dir.glob("*.parquet")):
            file_day = self._parse_date(path.stem)
            if file_day is None:
                continue
            if first_day <= file_day < end:
                try:
                    frames.append(pd.read_parquet(path))
                except Exception as exc:  # pragma: no cover - defensive
                    logger.warning("Ошибка чтения %s: %s", path, exc)
        frames = [f for f in frames if not f.empty]
        if not frames:
            return pd.DataFrame()
        df = pd.concat(frames, ignore_index=True)
        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
        mask = (df["timestamp"] >= pd.Timestamp(start)) & (df["timestamp"] < pd.Timestamp(end))
        return df.loc[mask].reset_index(drop=True)

    def parquet_day_path(self, day: str) -> Path | None:
        day_str = pd.Timestamp(day).strftime("%Y-%m-%d")
        path = self._storage_dir / f"{day_str}.parquet"
        return path if path.exists() else None

    def delete_day(self, day: str) -> None:
        """Удаляет день целиком из обоих хранилищ (строки SQLite и Parquet-файл).

        Нужен, когда официальный архив подтвердил, что биржевых сделок за день
        не было, а в хранилище лежат только отброшенные (дилерские) сделки.
        """
        self.replace_hot_day(day, [])
        path = self.parquet_day_path(day)
        if path is not None:
            path.unlink()
            logger.info("Удалён Parquet %s", path)

    # ------------------------------------------------------------------ #
    # Обнаружение / состояние
    # ------------------------------------------------------------------ #
    def available_days(self) -> set[str]:
        """Объединение горячих и холодных дней для этого инструмента."""
        return self.hot_days_available() | self.parquet_days_available()

    @staticmethod
    def _as_utc(value: datetime) -> datetime:
        if value.tzinfo is None:
            value = value.replace(tzinfo=UTC)
        return value.astimezone(UTC)

    @classmethod
    def _utc_midnight(cls, value: datetime) -> datetime:
        return cls._as_utc(value).replace(hour=0, minute=0, second=0, microsecond=0)

    @staticmethod
    def _parse_date(value: str) -> datetime | None:
        try:
            return datetime.fromisoformat(value).replace(tzinfo=UTC)
        except ValueError:
            return None
