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
import pyarrow as pa  # type: ignore[import-untyped]
import pyarrow.parquet as pq  # type: ignore[import-untyped]

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
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS degraded_minutes (
                minute  TEXT NOT NULL,
                reason  TEXT NOT NULL,
                PRIMARY KEY (minute, reason)
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS quality_events (
                ts       TEXT NOT NULL,
                state    TEXT NOT NULL,
                reasons  TEXT NOT NULL
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
    # Качество данных (SPEC-0004)
    # ------------------------------------------------------------------ #
    def save_quality(
        self, minutes: Sequence[tuple[str, str]], events: Sequence[tuple[str, str, str]]
    ) -> None:
        """Сохраняет отметки минут ``(минута, причина)`` и переходы состояния одной транзакцией."""
        if not minutes and not events:
            return
        with self.session() as conn:
            conn.execute("BEGIN TRANSACTION;")
            try:
                conn.executemany(
                    "INSERT OR IGNORE INTO degraded_minutes (minute, reason) VALUES (?, ?)",
                    list(minutes),
                )
                conn.executemany(
                    "INSERT INTO quality_events (ts, state, reasons) VALUES (?, ?, ?)",
                    list(events),
                )
                conn.commit()
            except Exception:
                conn.rollback()
                raise

    def read_degraded_minutes(self, start: datetime, end: datetime) -> pd.DataFrame:
        """Минуты ``DEGRADED`` в ``[start, end)``: колонки ``minute`` (UTC), ``reasons``."""
        start, end = self._as_utc(start), self._as_utc(end)
        with self.session() as conn:
            rows = conn.execute(
                "SELECT minute, group_concat(reason, ',') FROM degraded_minutes"
                " WHERE minute >= ? AND minute < ? GROUP BY minute ORDER BY minute",
                (start.replace(second=0, microsecond=0).isoformat(), end.isoformat()),
            ).fetchall()
        df = pd.DataFrame(rows, columns=["minute", "reasons"])
        df["minute"] = pd.to_datetime(df["minute"], utc=True, format="ISO8601")
        return df

    def quality_state(self) -> dict[str, object] | None:
        """Текущее состояние инструмента — последняя строка журнала ``quality_events``."""
        with self.session() as conn:
            row = conn.execute(
                "SELECT ts, state, reasons FROM quality_events ORDER BY rowid DESC LIMIT 1"
            ).fetchone()
        if row is None:
            return None
        return {"since": row[0], "state": row[1], "reasons": [r for r in row[2].split(",") if r]}

    def last_tick_time(self) -> datetime | None:
        """Время последней сделки в горячем хранилище (UTC) или ``None``."""
        with self.session() as conn:
            row = conn.execute(
                "SELECT MAX(timestamp) FROM ticks_hot_store WHERE ticker = ?",
                (self.instrument.storage_name,),
            ).fetchone()
        if row is None or row[0] is None:
            return None
        moment: datetime = pd.to_datetime(row[0], utc=True, format="ISO8601").to_pydatetime()
        return moment

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


#: Колонки ленты ISS в SQLite и Parquet (SPEC-0003).
ISS_COLUMNS = (
    "market",
    "feed_date",
    "secid",
    "board",
    "tradeno",
    "timestamp",
    "trade_date",
    "price",
    "quantity",
    "direction",
    "period",
    "session",
    "open_position",
    "off_market",
)
_ISS_EXPORT_CHUNK = 200_000


class IssTapeRepository:
    """Лента Мосбиржи из ISS (SPEC-0003): весь рынок, по одной ленте на рынок.

    Сделки текущей ленты копятся в SQLite ``_sqlite/_iss.sqlite3`` (таблица
    ``iss_trades``). Ключ уникальности (``market``, ``secid``, ``tradeno``)
    исключает дубли при повторной загрузке (SPEC-0003 G-4). После смены дня
    в ISS лента выгружается в ``_iss/{market}/{feed_date}.parquet``.
    ``feed_date`` — календарная дата (МСК) первой сделки ленты; торговый день
    биржи (``TRADE_SESSION_DATE``) хранится отдельно в ``trade_date``: сделки
    сессии выходного дня относятся к следующему торговому дню.
    """

    def __init__(self, repo_path: Path) -> None:
        sqlite_dir = repo_path / "_sqlite"
        sqlite_dir.mkdir(parents=True, exist_ok=True)
        self._db_path = sqlite_dir / "_iss.sqlite3"
        self._parquet_dir = repo_path / "_iss"

    @contextmanager
    def session(self) -> Iterator[sqlite3.Connection]:
        """SQLite-подключение в режиме WAL: сверка читает, пока сборщик пишет."""
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
            CREATE TABLE IF NOT EXISTS iss_trades (
                market        TEXT NOT NULL,
                feed_date     TEXT NOT NULL,
                secid         TEXT NOT NULL,
                board         TEXT NOT NULL,
                tradeno       INTEGER NOT NULL,
                timestamp     TEXT NOT NULL,
                trade_date    TEXT NOT NULL,
                price         REAL NOT NULL,
                quantity      INTEGER NOT NULL,
                direction     TEXT NOT NULL,
                period        TEXT,
                session       TEXT,
                open_position INTEGER,
                off_market    INTEGER,
                UNIQUE (market, secid, tradeno)
            )
            """
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_iss_feed ON iss_trades (market, feed_date)")
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS iss_feed_state (
                market     TEXT PRIMARY KEY,
                head       TEXT NOT NULL,
                feed_date  TEXT NOT NULL,
                offset     INTEGER NOT NULL,  -- сохранено строк ленты
                last       TEXT,              -- курсор: TRADENO (акции) / RECNO (FORTS)
                caught_up  INTEGER NOT NULL DEFAULT 0
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS iss_day_status (
                market      TEXT NOT NULL,
                day         TEXT NOT NULL,
                kind        TEXT NOT NULL,
                status      TEXT NOT NULL,
                detail      TEXT NOT NULL,
                checked_at  TEXT NOT NULL,
                PRIMARY KEY (market, day, kind)
            )
            """
        )
        conn.execute(
            "CREATE TABLE IF NOT EXISTS iss_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)"
        )

    # ------------------------------------------------------------------ #
    # Состояние ленты
    # ------------------------------------------------------------------ #
    def feed_state(self, market: str) -> dict[str, object] | None:
        with self.session() as conn:
            row = conn.execute(
                "SELECT head, feed_date, offset, last, caught_up FROM iss_feed_state"
                " WHERE market = ?",
                (market,),
            ).fetchone()
        if row is None:
            return None
        return {
            "head": row[0],
            "feed_date": row[1],
            "offset": int(row[2]),
            "last": row[3],
            "caught_up": bool(row[4]),
        }

    def start_feed(self, market: str, head: str, feed_date: str) -> None:
        """Начинает новую ленту рынка с нулевого курсора."""
        with self.session() as conn:
            conn.execute(
                "INSERT OR REPLACE INTO iss_feed_state"
                " (market, head, feed_date, offset, last, caught_up) VALUES (?, ?, ?, 0, NULL, 0)",
                (market, head, feed_date),
            )
            conn.commit()

    def set_caught_up(self, market: str, caught_up: bool) -> None:
        with self.session() as conn:
            conn.execute(
                "UPDATE iss_feed_state SET caught_up = ? WHERE market = ?",
                (int(caught_up), market),
            )
            conn.commit()

    def append_page(
        self, market: str, rows: Sequence[dict[str, object]], offset: int, last: str
    ) -> int:
        """Сохраняет страницу ленты и курсор в одной транзакции; возвращает число новых строк."""
        with self.session() as conn:
            conn.execute("BEGIN TRANSACTION;")
            try:
                inserted = self._insert_iss_rows(conn, rows)
                conn.execute(
                    "UPDATE iss_feed_state SET offset = ?, last = ?, caught_up = 0"
                    " WHERE market = ?",
                    (offset, last, market),
                )
                conn.commit()
            except Exception:
                conn.rollback()
                raise
        return inserted

    def stage_rows(self, rows: Sequence[dict[str, object]]) -> int:
        """Добавляет строки ленты без дублей; возвращает число новых строк."""
        if not rows:
            return 0
        with self.session() as conn:
            conn.execute("BEGIN TRANSACTION;")
            try:
                inserted = self._insert_iss_rows(conn, rows)
                conn.commit()
            except Exception:
                conn.rollback()
                raise
        return inserted

    @staticmethod
    def _insert_iss_rows(conn: sqlite3.Connection, rows: Sequence[dict[str, object]]) -> int:
        if not rows:
            return 0
        placeholders = ", ".join("?" for _ in ISS_COLUMNS)
        before = conn.total_changes
        conn.executemany(
            f"INSERT OR IGNORE INTO iss_trades ({', '.join(ISS_COLUMNS)}) VALUES ({placeholders})",
            [tuple(row.get(col) for col in ISS_COLUMNS) for row in rows],
        )
        return conn.total_changes - before

    # ------------------------------------------------------------------ #
    # Выгрузка дня в Parquet
    # ------------------------------------------------------------------ #
    def staged_feed_dates(self, market: str) -> set[str]:
        with self.session() as conn:
            cur = conn.execute(
                "SELECT DISTINCT feed_date FROM iss_trades WHERE market = ?", (market,)
            )
            return {str(r[0]) for r in cur.fetchall()}

    def parquet_path(self, market: str, feed_date: str) -> Path:
        return self._parquet_dir / market / f"{feed_date}.parquet"

    def parquet_feed_dates(self, market: str) -> set[str]:
        directory = self._parquet_dir / market
        if not directory.exists():
            return set()
        return {p.stem for p in directory.glob("*.parquet")}

    def finalize_feed_day(self, market: str, feed_date: str) -> Path | None:
        """Выгружает ленту дня из SQLite в Parquet и удаляет её из SQLite.

        Уже существующий Parquet дня сначала возвращается в SQLite, поэтому
        повторная выгрузка (например, после импорта сырых файлов) объединяет
        данные без дублей. Файл пишется частями во временный путь и подменяет
        старый только после проверки числа строк; иначе
        :class:`DataCorruptionError`, строки в SQLite остаются.
        """
        path = self.parquet_path(market, feed_date)
        if path.exists():
            for batch in pq.ParquetFile(path).iter_batches(batch_size=_ISS_EXPORT_CHUNK):
                self.stage_rows(self._rows_from_arrow(batch.to_pylist()))
        with self.session() as conn:
            total = int(
                conn.execute(
                    "SELECT COUNT(*) FROM iss_trades WHERE market = ? AND feed_date = ?",
                    (market, feed_date),
                ).fetchone()[0]
            )
            if total == 0:
                return None
            path.parent.mkdir(parents=True, exist_ok=True)
            tmp = path.with_suffix(".parquet.tmp")
            cur = conn.execute(
                f"SELECT {', '.join(ISS_COLUMNS)} FROM iss_trades"
                " WHERE market = ? AND feed_date = ? ORDER BY rowid",
                (market, feed_date),
            )
            with pq.ParquetWriter(tmp, _ISS_ARROW_SCHEMA, compression="snappy") as writer:
                while chunk := cur.fetchmany(_ISS_EXPORT_CHUNK):
                    writer.write_table(_iss_arrow_table(chunk))
            written = pq.read_metadata(tmp).num_rows
            if written != total:
                tmp.unlink(missing_ok=True)
                raise DataCorruptionError(
                    f"Лента ISS {market} {feed_date}: записано {written} строк из {total}"
                )
            tmp.replace(path)
            conn.execute(
                "DELETE FROM iss_trades WHERE market = ? AND feed_date = ?", (market, feed_date)
            )
            conn.commit()
        logger.info("Лента ISS %s за %s выгружена в %s (%d строк)", market, feed_date, path, total)
        return path

    @staticmethod
    def _rows_from_arrow(records: list[dict[str, Any]]) -> list[dict[str, object]]:
        for rec in records:
            ts = rec["timestamp"]
            rec["timestamp"] = ts.isoformat() if isinstance(ts, datetime) else str(ts)
        return cast(list[dict[str, object]], records)

    # ------------------------------------------------------------------ #
    # Чтение для проверки полноты
    # ------------------------------------------------------------------ #
    def _parquet_paths_since(self, market: str, since: str) -> list[Path]:
        return [
            self.parquet_path(market, d)
            for d in sorted(self.parquet_feed_dates(market))
            if d >= since
        ]

    def trade_dates(self, market: str, since: str) -> set[str]:
        """Торговые дни (``TRADEDATE``) в лентах с ``feed_date`` не раньше ``since``."""
        dates: set[str] = set()
        for path in self._parquet_paths_since(market, since):
            column = pq.read_table(path, columns=["trade_date"]).column("trade_date")
            dates.update(str(v) for v in column.unique().to_pylist())
        with self.session() as conn:
            cur = conn.execute(
                "SELECT DISTINCT trade_date FROM iss_trades WHERE market = ? AND feed_date >= ?",
                (market, since),
            )
            dates.update(str(r[0]) for r in cur.fetchall())
        return dates

    def count_trades(self, market: str, trade_date: str, board: str, since: str) -> dict[str, int]:
        """Число сделок по бумагам за торговый день в режиме ``board`` (лента + Parquet)."""
        counts: dict[str, int] = {}
        for path in self._parquet_paths_since(market, since):
            table = pq.read_table(
                path,
                columns=["secid"],
                filters=[("trade_date", "=", trade_date), ("board", "=", board)],
            )
            for secid, n in pd.Series(table.column("secid").to_pylist()).value_counts().items():
                counts[str(secid)] = counts.get(str(secid), 0) + int(n)
        with self.session() as conn:
            cur = conn.execute(
                "SELECT secid, COUNT(*) FROM iss_trades"
                " WHERE market = ? AND trade_date = ? AND board = ? GROUP BY secid",
                (market, trade_date, board),
            )
            for secid, n in cur.fetchall():
                counts[str(secid)] = counts.get(str(secid), 0) + int(n)
        return counts

    # ------------------------------------------------------------------ #
    # Статусы полноты и служебные флаги
    # ------------------------------------------------------------------ #
    def set_day_status(self, market: str, day: str, kind: str, status: str, detail: str) -> None:
        with self.session() as conn:
            conn.execute(
                "INSERT OR REPLACE INTO iss_day_status"
                " (market, day, kind, status, detail, checked_at) VALUES (?, ?, ?, ?, ?, ?)",
                (market, day, kind, status, detail, datetime.now(UTC).isoformat()),
            )
            conn.commit()

    def day_status(self, market: str, day: str, kind: str) -> str | None:
        with self.session() as conn:
            row = conn.execute(
                "SELECT status FROM iss_day_status WHERE market = ? AND day = ? AND kind = ?",
                (market, day, kind),
            ).fetchone()
        return None if row is None else str(row[0])

    def get_meta(self, key: str) -> str | None:
        with self.session() as conn:
            row = conn.execute("SELECT value FROM iss_meta WHERE key = ?", (key,)).fetchone()
        return None if row is None else str(row[0])

    def set_meta(self, key: str, value: str) -> None:
        with self.session() as conn:
            conn.execute("INSERT OR REPLACE INTO iss_meta (key, value) VALUES (?, ?)", (key, value))
            conn.commit()


_ISS_ARROW_SCHEMA = pa.schema(
    [
        ("market", pa.string()),
        ("feed_date", pa.string()),
        ("secid", pa.string()),
        ("board", pa.string()),
        ("tradeno", pa.int64()),
        ("timestamp", pa.timestamp("us", tz="UTC")),
        ("trade_date", pa.string()),
        ("price", pa.float64()),
        ("quantity", pa.int64()),
        ("direction", pa.string()),
        ("period", pa.string()),
        ("session", pa.string()),
        ("open_position", pa.int64()),
        ("off_market", pa.int64()),
    ]
)


def _iss_arrow_table(chunk: list[tuple[Any, ...]]) -> pa.Table:
    columns = list(zip(*chunk, strict=True))
    arrays: dict[str, Any] = {}
    for idx, name in enumerate(ISS_COLUMNS):
        values = list(columns[idx])
        if name == "timestamp":
            arrays[name] = pd.to_datetime(values, utc=True, format="ISO8601")
        else:
            arrays[name] = values
    return pa.Table.from_pydict(arrays, schema=_ISS_ARROW_SCHEMA)
