"""SPEC-0003: сбор ленты Мосбиржи из ISS."""

from __future__ import annotations

import gzip
import logging
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq
import pytest

from cluster_source import iss
from cluster_source.config import AppConfig
from cluster_source.database import IssTapeRepository
from cluster_source.iss import (
    KIND_FEED,
    KIND_TRADE_DATE,
    STATUS_COMPLETE,
    STATUS_INCOMPLETE,
    STATUS_UNVERIFIABLE,
    IssCollector,
    normalize_trades,
)

UTC = UTC

SHARES_COLUMNS = [
    "TRADENO", "TRADETIME", "SECID", "PRICE", "QUANTITY", "PERIOD", "VALUE", "BOARDID",
    "SYSTIME", "BUYSELL", "DECIMALS", "TRADINGSESSION", "TRADEDATE", "TRADE_SESSION_DATE",
]  # fmt: skip
FORTS_COLUMNS = [
    "TRADENO", "BOARDNAME", "SECID", "TRADEDATE", "TRADETIME", "PRICE", "QUANTITY", "SYSTIME",
    "RECNO", "OPENPOSITION", "OFFMARKETDEAL", "BUYSELL", "TRADE_SESSION_DATE",
]  # fmt: skip


def share(
    no: int,
    tradetime: str = "10:00:00",
    secid: str = "SBER",
    day: str = "2026-10-12",
    trade_date: str | None = None,
    side: str = "B",
    board: str = "TQBR",
) -> list[object]:
    td = trade_date or day
    systime = f"{day} {tradetime}"
    return [no, tradetime, secid, 285.5, 10, "N", 2855.0, board, systime, side, 2, "1", td, td]


def fut(
    no: int, tradetime: str = "10:00:00", day: str = "2026-10-12", oi: int = 100
) -> list[object]:
    return [no, "RFUD", "SiZ6", day, tradetime, 85053, 3, f"{day} {tradetime}", no, oi, 0, "S", day]


class FakeIss:
    """Лента ISS в памяти: курсоры tradeno/recno, сбои, смена дня, дневные итоги."""

    def __init__(self) -> None:
        self.feeds: dict[str, list[list[object]]] = {"shares": [], "forts": []}
        self.empty_pages = 0  # столько ответов на запрос с курсором вернутся пустыми
        self.partial_pages = 0  # столько ответов вернут одну строку вместо limit
        self.history: dict[tuple[str, str], dict[str, int]] = {}

    async def get_json(self, url: str, params: dict[str, str]) -> dict[str, Any]:
        market = "shares" if "/stock/" in url else "forts"
        if "/history/" in url:
            start = int(params.get("start", "0"))
            items = sorted(self.history.get((market, params["date"]), {}).items())
            page = items[start : start + 1]
            return {
                "history": {
                    "columns": ["BOARDID", "SECID", "NUMTRADES"],
                    "data": [["X", s, n] for s, n in page],
                },
                "history.cursor": {"data": [[start, len(items), 1]]},
            }
        assert "start" not in params, "лента читается курсором, а не устаревшим start"
        columns = SHARES_COLUMNS if market == "shares" else FORTS_COLUMNS
        feed = self.feeds[market]
        limit = int(params["limit"])
        cursor = params.get("tradeno") or params.get("recno")
        pos = 0
        if cursor is not None:
            col = columns.index("TRADENO" if "tradeno" in params else "RECNO")
            found = [i for i, r in enumerate(feed) if str(r[col]) == cursor]
            pos = found[0] + (1 if params.get("next_trade") else 0) if found else len(feed)
        if cursor is not None and self.empty_pages > 0:
            self.empty_pages -= 1
            rows: list[list[object]] = []
        elif self.partial_pages > 0:
            self.partial_pages -= 1
            rows = feed[pos : pos + 1]
        else:
            rows = feed[pos : pos + limit]
        return {"trades": {"columns": columns, "data": [list(r) for r in rows]}}


async def _no_sleep(_: float) -> None:
    return None


@pytest.fixture
def iss_repo(share_config: AppConfig) -> IssTapeRepository:
    return IssTapeRepository(share_config.storage_root)


@pytest.fixture(autouse=True)
def small_pages(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(iss, "PAGE", 2)


def collector(
    config: AppConfig, repo: IssTapeRepository, fake: FakeIss, now: datetime | None = None
) -> IssCollector:
    moment = now or datetime(2026, 10, 12, 17, 0, tzinfo=UTC)
    return IssCollector(config, repo, fake, now=lambda: moment, sleep=_no_sleep)


def staged(repo: IssTapeRepository, market: str = "shares") -> int:
    with repo.session() as conn:
        return int(
            conn.execute("SELECT COUNT(*) FROM iss_trades WHERE market = ?", (market,)).fetchone()[
                0
            ]
        )


# --------------------------------------------------------------------------- #
# A-4: нормализация
# --------------------------------------------------------------------------- #
def test_normalize_shares_time_direction_and_fields() -> None:
    """SPEC-0003 A-4: МСК → UTC, B/S → BUY/SELL, поля режима и сессии."""
    rows = normalize_trades(
        "shares", "2026-10-12", SHARES_COLUMNS, [share(17876835746, "06:52:12", side="S")]
    )
    row = rows[0]
    assert row["timestamp"] == "2026-10-12T03:52:12.000000+00:00"
    assert row["direction"] == "SELL"
    assert row["tradeno"] == 17876835746
    assert (row["secid"], row["board"], row["period"], row["session"]) == ("SBER", "TQBR", "N", "1")
    assert row["open_position"] is None


def test_normalize_forts_and_csv_strings() -> None:
    """SPEC-0003 A-4: FORTS — режим из BOARDNAME, ОИ и внебиржевой признак; CSV-строки."""
    as_csv = [str(v) for v in fut(1892952147893622568, "19:20:56", oi=9079754)]
    row = normalize_trades("forts", "2026-10-12", FORTS_COLUMNS, [as_csv])[0]
    assert row["tradeno"] == 1892952147893622568  # большие номера без потери точности
    assert row["board"] == "RFUD"
    assert row["open_position"] == 9079754
    assert row["off_market"] == 0
    assert row["direction"] == "SELL"
    assert row["timestamp"] == "2026-10-12T16:20:56.000000+00:00"


def test_normalize_forts_weekend_batch_loaded_on_monday() -> None:
    """SPEC-0003 A-4: сделки сессии выходного дня FORTS приходят пачкой в понедельник 06:00.

    Реальная строка ленты 2026-10-12: TRADEDATE — суббота, SYSTIME — понедельник,
    TRADE_SESSION_DATE — понедельник. Время сделки — суббота, торговый день — понедельник.
    """
    raw = ["2014267862855712769", "RFUD", "AKZ6", "2026-10-10", "09:59:12", "7474", "3",
           "2026-10-12 06:00:14", "349212905121", "89376", "0", "B", "2026-10-12"]  # fmt: skip
    row = normalize_trades("forts", "2026-10-12", FORTS_COLUMNS, [raw])[0]
    assert row["timestamp"] == "2026-10-10T06:59:12.000000+00:00"
    assert row["trade_date"] == "2026-10-12"


def test_normalize_weekend_session_and_midnight() -> None:
    """SPEC-0003 A-4: дата сделки — из SYSTIME (сессия выходного дня), переход через полночь."""
    weekend = share(1, "12:00:00", day="2026-10-10", trade_date="2026-10-12")
    row = normalize_trades("shares", "2026-10-10", SHARES_COLUMNS, [weekend])[0]
    assert row["timestamp"].startswith("2026-10-10T09:00:00")  # type: ignore[union-attr]
    assert row["trade_date"] == "2026-10-12"

    late = share(2, "23:59:59", day="2026-10-12")
    late[SHARES_COLUMNS.index("SYSTIME")] = "2026-10-13 00:00:01"
    row = normalize_trades("shares", "2026-10-12", SHARES_COLUMNS, [late])[0]
    assert row["timestamp"].startswith("2026-10-12T20:59:59")  # type: ignore[union-attr]


# --------------------------------------------------------------------------- #
# A-1: догонка ленты
# --------------------------------------------------------------------------- #
async def test_catch_up_through_partial_and_empty_pages(
    share_config: AppConfig, iss_repo: IssTapeRepository
) -> None:
    """SPEC-0003 A-1: курсор tradeno: неполные страницы и редкие пустые ответы без потерь."""
    fake = FakeIss()
    fake.feeds["shares"] = [share(100 + i) for i in range(7)]
    fake.empty_pages = 2
    fake.partial_pages = 2
    await collector(share_config, iss_repo, fake).poll_market("shares")

    state = iss_repo.feed_state("shares")
    assert state is not None
    assert staged(iss_repo) == 7
    assert state["offset"] == 7
    assert state["last"] == "106"
    assert state["caught_up"] is True


async def test_next_cycle_continues_from_cursor(
    share_config: AppConfig, iss_repo: IssTapeRepository
) -> None:
    """SPEC-0003 A-1: следующий цикл продолжает с курсора, новые сделки сбрасывают caught_up."""
    fake = FakeIss()
    fake.feeds["shares"] = [share(100 + i) for i in range(3)]
    col = collector(share_config, iss_repo, fake)
    await col.poll_market("shares")
    fake.feeds["shares"] += [share(200), share(201)]
    await col.poll_market("shares")
    assert staged(iss_repo) == 5
    assert iss_repo.feed_state("shares")["offset"] == 5  # type: ignore[index]


async def test_false_end_of_feed_loses_nothing(
    share_config: AppConfig, iss_repo: IssTapeRepository
) -> None:
    """SPEC-0003 A-1: ложный «конец ленты» при сбое ISS — следующий цикл дочитывает с курсора."""
    fake = FakeIss()
    fake.feeds["shares"] = [share(100 + i) for i in range(5)]
    fake.empty_pages = 4  # больше, чем подтверждений конца ленты
    col = collector(share_config, iss_repo, fake)
    await col.poll_market("shares")
    assert staged(iss_repo) == 2
    await col.poll_market("shares")
    assert staged(iss_repo) == 5


async def test_forts_cursor_is_recno(share_config: AppConfig, iss_repo: IssTapeRepository) -> None:
    """SPEC-0003 A-1: на FORTS курсор — RECNO (TRADENO там не монотонен)."""
    fake = FakeIss()
    fake.feeds["forts"] = [fut(30), fut(10), fut(20)]
    for row, recno in zip(fake.feeds["forts"], (1, 2, 3), strict=True):
        row[FORTS_COLUMNS.index("RECNO")] = recno
    await collector(share_config, iss_repo, fake).poll_market("forts")
    state = iss_repo.feed_state("forts")
    assert state is not None
    assert (state["offset"], state["last"], state["caught_up"]) == (3, "3", True)
    assert staged(iss_repo, "forts") == 3


# --------------------------------------------------------------------------- #
# A-2: смена дня
# --------------------------------------------------------------------------- #
async def test_day_change_finalizes_previous_feed(
    share_config: AppConfig, iss_repo: IssTapeRepository
) -> None:
    """SPEC-0003 A-2: смена первой сделки ленты — прежний день уходит в Parquet."""
    fake = FakeIss()
    fake.feeds["shares"] = [share(100 + i) for i in range(3)]
    col = collector(share_config, iss_repo, fake)
    await col.poll_market("shares")

    fake.feeds["shares"] = [share(500, day="2026-10-13"), share(501, day="2026-10-13")]
    await col.poll_market("shares")

    path = iss_repo.parquet_path("shares", "2026-10-12")
    assert pq.read_metadata(path).num_rows == 3
    assert iss_repo.staged_feed_dates("shares") == {"2026-10-13"}
    assert iss_repo.feed_state("shares")["feed_date"] == "2026-10-13"  # type: ignore[index]
    assert iss_repo.day_status("shares", "2026-10-12", KIND_FEED) == STATUS_COMPLETE


async def test_day_change_before_catch_up_is_marked_incomplete(
    share_config: AppConfig, iss_repo: IssTapeRepository, caplog: pytest.LogCaptureFixture
) -> None:
    """SPEC-0003 G-2: ISS сменил день до догонки — день помечен неполным, в логе маркер."""
    fake = FakeIss()
    fake.feeds["shares"] = [share(100 + i) for i in range(5)]
    col = collector(share_config, iss_repo, fake)
    await col.poll_market("shares")
    iss_repo.set_caught_up("shares", False)  # как если бы догонка оборвалась

    fake.feeds["shares"] = [share(500, day="2026-10-13")]
    with caplog.at_level(logging.ERROR):
        await col.poll_market("shares")
    assert iss_repo.day_status("shares", "2026-10-12", KIND_FEED) == STATUS_INCOMPLETE
    assert "ISS_FEED_INCOMPLETE" in caplog.text


async def test_stale_staged_feed_finalized_on_housekeeping(
    share_config: AppConfig, iss_repo: IssTapeRepository
) -> None:
    """SPEC-0003: лента, оставшаяся в SQLite после падения, выгружается при обслуживании."""
    iss_repo.stage_rows(
        normalize_trades("shares", "2026-10-09", SHARES_COLUMNS, [share(1, day="2026-10-09")])
    )
    await collector(share_config, iss_repo, FakeIss()).finalize_stale_feeds()
    assert iss_repo.parquet_feed_dates("shares") == {"2026-10-09"}
    assert staged(iss_repo) == 0


# --------------------------------------------------------------------------- #
# A-3: без дублей
# --------------------------------------------------------------------------- #
def test_restaging_and_refinalizing_do_not_duplicate(iss_repo: IssTapeRepository) -> None:
    """SPEC-0003 A-3 (G-4): ключ (market, secid, tradeno) исключает дубли, Parquet объединяется."""
    rows = normalize_trades("shares", "2026-10-12", SHARES_COLUMNS, [share(1), share(2)])
    assert iss_repo.stage_rows(rows) == 2
    assert iss_repo.stage_rows(rows) == 0
    iss_repo.finalize_feed_day("shares", "2026-10-12")

    more = normalize_trades("shares", "2026-10-12", SHARES_COLUMNS, [share(2), share(3)])
    iss_repo.stage_rows(more)
    path = iss_repo.finalize_feed_day("shares", "2026-10-12")
    assert path is not None
    table = pq.read_table(path)
    assert sorted(table.column("tradeno").to_pylist()) == [1, 2, 3]
    assert str(table.schema.field("timestamp").type) == "timestamp[us, tz=UTC]"


# --------------------------------------------------------------------------- #
# Импорт сырых файлов этапа 1
# --------------------------------------------------------------------------- #
def _write_raw(path: Path, members: list[list[list[object]]], tail: bytes = b"") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    blob = b""
    for rows in members:
        lines = [",".join(SHARES_COLUMNS)] + [",".join(str(v) for v in r) for r in rows]
        blob += gzip.compress(("\n".join(lines) + "\n").encode())
    path.write_bytes(blob + tail)


async def test_import_raw_stage1_files(
    share_config: AppConfig, iss_repo: IssTapeRepository
) -> None:
    """SPEC-0003: сырые файлы этапа 1 импортируются без дублей и только после смены дня в ISS."""
    raw = share_config.storage_root / "_iss_raw"
    day = "2026-10-11"
    _write_raw(
        raw / day / "shares.csv.gz",
        [[share(1, day=day), share(2, day=day)], [share(2, day=day), share(3, day=day)]],
    )
    _write_raw(raw / "2026-10-12" / "shares.csv.gz", [[share(9)]])
    col = collector(share_config, iss_repo, FakeIss())

    await col.import_raw()  # сборщик ещё не видел ленту: день может идти
    assert iss_repo.parquet_feed_dates("shares") == set()

    iss_repo.start_feed("shares", "9|2026-10-12 10:00:00", "2026-10-12")
    await col.import_raw()
    await col.import_raw()

    path = iss_repo.parquet_path("shares", day)
    assert sorted(pq.read_table(path).column("tradeno").to_pylist()) == [1, 2, 3]
    assert "2026-10-12" not in iss_repo.parquet_feed_dates("shares")  # лента дня ещё идёт
    assert iss_repo.day_status("shares", day, KIND_FEED) is None


async def test_import_reimports_grown_file_and_extra_files(
    share_config: AppConfig, iss_repo: IssTapeRepository
) -> None:
    """SPEC-0003: выросший сырой файл и дополнительные файлы рынка дня импортируются."""
    raw = share_config.storage_root / "_iss_raw" / "2026-10-11"
    day = "2026-10-11"
    iss_repo.start_feed("shares", "9|2026-10-12 10:00:00", "2026-10-12")
    col = collector(share_config, iss_repo, FakeIss())
    _write_raw(raw / "shares.csv.gz", [[share(1, day=day)]])
    await col.import_raw()
    _write_raw(raw / "shares.csv.gz", [[share(1, day=day), share(2, day=day)]])
    _write_raw(raw / "shares_prev_session.csv.gz", [[share(3, day=day)]])
    await col.import_raw()
    path = iss_repo.parquet_path("shares", day)
    assert sorted(pq.read_table(path).column("tradeno").to_pylist()) == [1, 2, 3]


async def test_import_truncated_raw_marks_incomplete(
    share_config: AppConfig, iss_repo: IssTapeRepository
) -> None:
    """SPEC-0003 G-2: обрезанный сырой файл импортируется частично, день помечен неполным."""
    day = "2026-10-11"
    _write_raw(
        share_config.storage_root / "_iss_raw" / day / "shares.csv.gz",
        [[share(1, day=day)]],
        tail=gzip.compress(b"x" * 1000)[:20],
    )
    iss_repo.start_feed("shares", "9|2026-10-12 10:00:00", "2026-10-12")
    await collector(share_config, iss_repo, FakeIss()).import_raw()
    assert pq.read_metadata(iss_repo.parquet_path("shares", day)).num_rows == 1
    assert iss_repo.day_status("shares", day, KIND_FEED) == STATUS_INCOMPLETE


# --------------------------------------------------------------------------- #
# G-5: полнота по дневным итогам
# --------------------------------------------------------------------------- #
def _tape(repo: IssTapeRepository, feed_date: str, rows: list[list[object]]) -> None:
    repo.stage_rows(normalize_trades("shares", feed_date, SHARES_COLUMNS, rows))
    repo.finalize_feed_day("shares", feed_date)


async def test_verify_complete_day_with_weekend_session(
    share_config: AppConfig, iss_repo: IssTapeRepository, caplog: pytest.LogCaptureFixture
) -> None:
    """SPEC-0003 G-5: сделки субботней ленты считаются в торговый день понедельника."""
    _tape(iss_repo, "2026-10-10", [share(1, day="2026-10-10", trade_date="2026-10-12")])
    _tape(
        iss_repo,
        "2026-10-12",
        [share(2), share(3, secid="GAZP"), share(4, board="SMAL")],  # SMAL не сверяется
    )
    fake = FakeIss()
    fake.history[("shares", "2026-10-12")] = {"SBER": 2, "GAZP": 1}
    with caplog.at_level(logging.INFO):
        await collector(
            share_config, iss_repo, fake, now=datetime(2026, 10, 13, 6, 0, tzinfo=UTC)
        ).verify_pending()
    assert iss_repo.day_status("shares", "2026-10-12", KIND_TRADE_DATE) == STATUS_COMPLETE
    assert "ISS_DAY_VERIFIED" in caplog.text


async def test_verify_detects_missing_trades(
    share_config: AppConfig, iss_repo: IssTapeRepository, caplog: pytest.LogCaptureFixture
) -> None:
    """SPEC-0003 G-5: расхождение с NUMTRADES — день неполный, маркер ISS_DAY_INCOMPLETE."""
    _tape(iss_repo, "2026-10-12", [share(2)])
    fake = FakeIss()
    fake.history[("shares", "2026-10-12")] = {"SBER": 5, "GAZP": 0}
    with caplog.at_level(logging.ERROR):
        await collector(
            share_config, iss_repo, fake, now=datetime(2026, 10, 13, 6, 0, tzinfo=UTC)
        ).verify_pending()
    assert iss_repo.day_status("shares", "2026-10-12", KIND_TRADE_DATE) == STATUS_INCOMPLETE
    assert "ISS_DAY_INCOMPLETE" in caplog.text
    assert "SBER: у нас 1, биржа 5" in caplog.text


async def test_verify_waits_for_daily_totals(
    share_config: AppConfig, iss_repo: IssTapeRepository
) -> None:
    """SPEC-0003 G-5: до 06:00 МСК следующих суток не проверяем; без итогов 3 дня — не сверить."""
    _tape(iss_repo, "2026-10-12", [share(2)])
    fake = FakeIss()
    fake.history[("shares", "2026-10-12")] = {"SBER": 1}
    await collector(
        share_config, iss_repo, fake, now=datetime(2026, 10, 12, 22, 0, tzinfo=UTC)
    ).verify_pending()
    assert iss_repo.day_status("shares", "2026-10-12", KIND_TRADE_DATE) is None

    fake.history.clear()
    await collector(
        share_config, iss_repo, fake, now=datetime(2026, 10, 15, 6, 0, tzinfo=UTC)
    ).verify_pending()
    assert iss_repo.day_status("shares", "2026-10-12", KIND_TRADE_DATE) == STATUS_UNVERIFIABLE


async def test_verify_forts_allows_trades_outside_anonymous_tape(
    share_config: AppConfig, iss_repo: IssTapeRepository
) -> None:
    """SPEC-0006 / SPEC-0003 G-5: в итогах FORTS есть сделки вне анонимной ленты (до 1%)."""
    rows = [fut(k, day="2026-10-12") for k in range(1, 100)]
    iss_repo.stage_rows(normalize_trades("forts", "2026-10-12", FORTS_COLUMNS, rows))
    iss_repo.finalize_feed_day("forts", "2026-10-12")
    fake = FakeIss()
    fake.history[("forts", "2026-10-12")] = {"SiZ6": 100}  # 99 в ленте + 1 блочная
    now = datetime(2026, 10, 13, 6, 0, tzinfo=UTC)
    await collector(share_config, iss_repo, fake, now=now).verify_pending()
    assert iss_repo.day_status("forts", "2026-10-12", KIND_TRADE_DATE) == STATUS_COMPLETE

    with iss_repo.session() as conn:
        conn.execute("DELETE FROM iss_day_status")
        conn.commit()
    fake.history[("forts", "2026-10-12")] = {"SiZ6": 200}  # лента усечена вдвое
    await collector(share_config, iss_repo, fake, now=now).verify_pending()
    assert iss_repo.day_status("forts", "2026-10-12", KIND_TRADE_DATE) == STATUS_INCOMPLETE
