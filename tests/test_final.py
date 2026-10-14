"""SPEC-0006: ночной статус FINAL торгового дня."""

from __future__ import annotations

import logging
from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from typing import Any

import pytest

from cluster_source.config import AppConfig, FuturesMeta, Instrument, InstrumentType
from cluster_source.database import IssTapeRepository, MarketDataRepository
from cluster_source.final import (
    ARCHIVE_EXHAUSTED,
    REFRESH_FINAL_MISMATCH,
    STATUS_ARCHIVE_CONSISTENT,
    STATUS_FINAL,
    STATUS_MISMATCH,
    STATUS_UNVERIFIABLE,
    run_final_check,
    trading_spans,
)
from cluster_source.iss import KIND_FEED, STATUS_COMPLETE
from cluster_source.reader import (
    baseline_quality,
    get_baseline_days,
    get_day_verdicts,
    get_final_days,
)

UTC = UTC
GAZP = Instrument(id="GAZP", type=InstrumentType.SHARE, tick_size=0.01, lot_size=10)
SI = Instrument(id="Si", type=InstrumentType.CONTINUOUS_FUTURES, tick_size=1, lot_size=1)
FUTURES = {"SiZ6": FuturesMeta("SiZ6", "SPBFUT", date(2026, 12, 18), "F_SIZ6")}
TODAY = date(2026, 10, 14)


class FakeIss:
    """Дневные итоги ISS по датам (постранично по 2 бумаги) и минутные свечи.

    ``candles`` — ``{SECID: [(начало минуты МСК, объём, максимум, минимум), …]}``.
    """

    def __init__(
        self,
        totals: dict[tuple[str, str], dict[str, tuple[int, float]]],
        candles: dict[str, list[tuple[str, float, float, float]]] | None = None,
    ) -> None:
        self.totals = totals
        self.candles = candles or {}
        self.calls = 0

    async def get_json(self, url: str, params: dict[str, str]) -> dict[str, Any]:
        self.calls += 1
        if url.endswith("/candles.json"):
            secid = url.split("/securities/")[1].split("/")[0]
            return {
                "candles": {
                    "columns": ["open", "close", "high", "low", "value", "volume", "begin", "end"],
                    "data": [
                        [lo, hi, hi, lo, 0, v, begin, begin[:17] + "59"]
                        for begin, v, hi, lo in self.candles.get(secid, [])
                        if begin >= params["from"]
                    ],
                }
            }
        market = "shares" if "/stock/" in url else "forts"
        items = sorted(self.totals.get((market, params["date"]), {}).items())
        start = int(params.get("start", "0"))
        page = items[start : start + 2]
        return {
            "history": {
                "columns": ["SECID", "NUMTRADES", "VOLUME"],
                "data": [[s, n, v] for s, (n, v) in page],
            },
            "history.cursor": {"data": [[start, len(items), 2]]},
        }


def ticks(name: str, day: date, n: int, volume: int = 1) -> list[dict[str, object]]:
    base = datetime(day.year, day.month, day.day, 8, tzinfo=UTC)
    return [
        {
            "ticker": name,
            "timestamp": base + timedelta(seconds=i),
            "price": 100.0,
            "volume": volume,
            "direction": "BUY",
            "trade_id": f"s{i}",
        }
        for i in range(n)
    ]


@pytest.fixture
def cfg(share_config: AppConfig) -> AppConfig:
    return replace(share_config, instruments=[GAZP, SI])


@pytest.fixture
def repos(cfg: AppConfig) -> dict[str, MarketDataRepository]:
    return {i.storage_name: MarketDataRepository(cfg.storage_root, i) for i in cfg.instruments}


@pytest.fixture
def iss(cfg: AppConfig) -> IssTapeRepository:
    return IssTapeRepository(cfg.storage_root)


async def check(cfg: AppConfig, repos: Any, iss: IssTapeRepository, fake: FakeIss) -> Any:
    return await run_final_check(cfg, repos, iss, fake, FUTURES, TODAY)


def status_of(repo: MarketDataRepository, trade_date: str) -> str:
    df = get_day_verdicts(repo)
    return str(df.loc[df["trade_date"] == trade_date, "status"].iloc[0])


# --------------------------------------------------------------------------- #
async def test_shares_exact_match_is_final_and_clears_refresh(
    cfg: AppConfig, repos: Any, iss: IssTapeRepository
) -> None:
    """SPEC-0006 A-1: акции — число и объём × лот совпали → FINAL, перекачка снята."""
    d = date(2026, 10, 13)
    repos["GAZP"].insert_ticks(ticks("GAZP", d, 5, volume=3))
    repos["GAZP"].mark_days_for_refresh([d.isoformat()], "stream_gap")
    fake = FakeIss({("shares", d.isoformat()): {"GAZP": (5, 150.0), "SBER": (9, 9.0)}})
    await check(cfg, repos, iss, fake)
    assert status_of(repos["GAZP"], "2026-10-13") == STATUS_FINAL
    assert repos["GAZP"].days_to_refresh() == {}
    assert get_final_days(repos["GAZP"]) == [d]


async def test_mismatch_logged_and_marked_for_refresh(
    cfg: AppConfig, repos: Any, iss: IssTapeRepository, caplog: pytest.LogCaptureFixture
) -> None:
    """SPEC-0006 A-2 (G-1): расхождение → MISMATCH, ERROR, отметка перекачки."""
    d = date(2026, 10, 13)
    repos["GAZP"].insert_ticks(ticks("GAZP", d, 4))
    fake = FakeIss({("shares", d.isoformat()): {"GAZP": (5, 50.0)}})
    with caplog.at_level(logging.ERROR):
        await check(cfg, repos, iss, fake)
    assert status_of(repos["GAZP"], "2026-10-13") == STATUS_MISMATCH
    assert "DAY_NOT_FINAL GAZP 2026-10-13" in caplog.text
    assert repos["GAZP"].days_to_refresh() == {"2026-10-13": REFRESH_FINAL_MISMATCH}
    assert get_final_days(repos["GAZP"]) == []


async def test_monday_includes_weekend_sessions(
    cfg: AppConfig, repos: Any, iss: IssTapeRepository
) -> None:
    """SPEC-0006 A-3: торговый день понедельника включает субботу и воскресенье."""
    fri, sat, sun, mon = (date(2026, 10, d) for d in (9, 10, 11, 12))
    for d, n in ((fri, 3), (sat, 2), (sun, 1), (mon, 4)):
        repos["GAZP"].insert_ticks(ticks("GAZP", d, n))
    fake = FakeIss(
        {
            ("shares", fri.isoformat()): {"GAZP": (3, 30.0)},
            ("shares", mon.isoformat()): {"GAZP": (7, 70.0)},  # 2 + 1 + 4
        }
    )
    await check(cfg, repos, iss, fake)
    assert status_of(repos["GAZP"], "2026-10-09") == STATUS_FINAL
    assert status_of(repos["GAZP"], "2026-10-12") == STATUS_FINAL
    assert set(get_day_verdicts(repos["GAZP"])["trade_date"]) == {"2026-10-09", "2026-10-12"}


def test_trading_spans() -> None:
    days = [date(2026, 10, 9) + timedelta(days=k) for k in range(5)]  # пт..вт
    spans = trading_spans(days, {date(2026, 10, 9), date(2026, 10, 12)})
    assert spans[date(2026, 10, 12)] == [date(2026, 10, 10), date(2026, 10, 11), date(2026, 10, 12)]
    assert date(2026, 10, 13) not in spans  # итоги ещё не вышли — ни к чему не относится


async def test_forts_reference_is_complete_tape(
    cfg: AppConfig, repos: Any, iss: IssTapeRepository
) -> None:
    """SPEC-0006 A-4: фьючерсы — эталон полная лента ISS, разница с итогами — справка."""
    d = date(2026, 10, 13)
    repos["Si_CONTINUOUS"].insert_ticks(ticks("Si_CONTINUOUS", d, 3, volume=2))
    rows = []
    for k in range(3):
        rows.append(
            {
                "market": "forts", "feed_date": d.isoformat(), "secid": "SiZ6", "board": "RFUD",
                "tradeno": k + 1, "timestamp": f"{d}T08:00:0{k}.000000+00:00",
                "trade_date": d.isoformat(), "price": 1.0, "quantity": 2, "direction": "BUY",
                "period": None, "session": None, "open_position": 1, "off_market": 0,
            }
        )  # fmt: skip
    iss.stage_rows(rows)
    iss.set_day_status("forts", d.isoformat(), KIND_FEED, STATUS_COMPLETE, "")
    fake = FakeIss(
        {("forts", d.isoformat()): {"SiZ6": (4, 500.0)}},  # +1 блочная сделка
        {"SiZ6": [("2026-10-13 11:00:00", 6.0, 100.0, 100.0)]},  # минута среза — по свече
    )
    await check(cfg, repos, iss, fake)
    df = get_day_verdicts(repos["Si_CONTINUOUS"])
    row = df.iloc[0]
    assert (row["status"], row["reference"], row["ref_n"]) == (STATUS_FINAL, "iss_tape", 3)
    assert "вне анонимной ленты: 1 сделок" in row["detail"]
    assert "хвост по свечам с 11:00 МСК" in row["detail"]


@pytest.mark.parametrize(
    ("ours", "official", "expected"),
    [
        (996, (1000, 2000.0), STATUS_ARCHIVE_CONSISTENT),  # недостача 0,4% — блочные сделки
        (900, (1000, 2000.0), STATUS_MISMATCH),  # 10% — дыра в данных
        (1001, (1000, 2000.0), STATUS_MISMATCH),  # у нас больше биржи
        (5, (0, 0.0), STATUS_UNVERIFIABLE),  # итогов нет
    ],
)
async def test_forts_without_complete_tape(
    cfg: AppConfig,
    repos: Any,
    iss: IssTapeRepository,
    ours: int,
    official: tuple[int, float],
    expected: str,
) -> None:
    """SPEC-0006 A-5 (G-3): без полной ленты — не FINAL; согласие с итогами в пределах
    сделок вне анонимной ленты — ARCHIVE_CONSISTENT, иначе MISMATCH; нет итогов — UNVERIFIABLE."""
    d = date(2026, 10, 9)
    repos["Si_CONTINUOUS"].insert_ticks(ticks("Si_CONTINUOUS", d, ours))
    totals = {"SiZ6": official} if official[0] else {"OTHER": (1, 1.0)}
    await check(cfg, repos, iss, FakeIss({("forts", d.isoformat()): totals}))
    assert status_of(repos["Si_CONTINUOUS"], "2026-10-09") == expected
    assert get_final_days(repos["Si_CONTINUOUS"]) == []
    marked = repos["Si_CONTINUOUS"].days_to_refresh()
    assert (marked == {"2026-10-09": REFRESH_FINAL_MISMATCH}) == (expected == STATUS_MISMATCH)


async def test_baseline_window_prefers_final(
    cfg: AppConfig, repos: Any, iss: IssTapeRepository
) -> None:
    """SPEC-0006 / SPEC-0008: окно норм — FINAL, добор ARCHIVE_CONSISTENT, без MISMATCH."""
    repo = repos["Si_CONTINUOUS"]
    statuses = {
        "2026-10-06": STATUS_ARCHIVE_CONSISTENT,
        "2026-10-07": STATUS_MISMATCH,
        "2026-10-08": STATUS_ARCHIVE_CONSISTENT,
        "2026-10-09": STATUS_ARCHIVE_CONSISTENT,
        "2026-10-13": STATUS_FINAL,
        "2026-10-14": STATUS_FINAL,
    }
    for d, st in statuses.items():
        repo.set_day_verdict(d, st, (1, 1.0), (1, 1.0), "x", "")
    window = get_baseline_days(repo, n=4)
    assert window == [
        (date(2026, 10, 8), STATUS_ARCHIVE_CONSISTENT),
        (date(2026, 10, 9), STATUS_ARCHIVE_CONSISTENT),
        (date(2026, 10, 13), STATUS_FINAL),
        (date(2026, 10, 14), STATUS_FINAL),
    ]
    assert baseline_quality(window) == "mixed"
    assert baseline_quality(get_baseline_days(repo, n=2)) == "pure"
    assert get_baseline_days(repo, n=4, allow_archive=False) == window[2:]
    assert baseline_quality([]) == "empty"


async def test_unpublished_totals_not_cached_as_non_trading(
    cfg: AppConfig, repos: Any, iss: IssTapeRepository
) -> None:
    """SPEC-0006 A-6: итогов вчера ещё нет — дата не кэшируется как неторговая."""
    d = date(2026, 10, 13)
    repos["GAZP"].insert_ticks(ticks("GAZP", d, 2))
    await check(cfg, repos, iss, FakeIss({}))
    assert get_day_verdicts(repos["GAZP"]).empty
    assert iss.cached_totals("shares", d.isoformat()) is None

    await check(cfg, repos, iss, FakeIss({("shares", d.isoformat()): {"GAZP": (2, 20.0)}}))
    assert status_of(repos["GAZP"], "2026-10-13") == STATUS_FINAL


async def test_final_days_not_rechecked(cfg: AppConfig, repos: Any, iss: IssTapeRepository) -> None:
    """SPEC-0006 A-7: FINAL-день повторно не проверяется; итоги берутся из кэша."""
    d = date(2026, 10, 13)
    repos["GAZP"].insert_ticks(ticks("GAZP", d, 2))
    fake = FakeIss({("shares", d.isoformat()): {"GAZP": (2, 20.0)}})
    await check(cfg, repos, iss, fake)
    first = get_day_verdicts(repos["GAZP"])
    calls = fake.calls
    stats = await check(cfg, repos, iss, fake)
    assert not stats
    assert fake.calls == calls  # итоги — из кэша
    assert list(get_day_verdicts(repos["GAZP"])["checked_at"]) == list(first["checked_at"])


def test_final_mismatch_mark_not_limited_by_window(cfg: AppConfig, repos: Any) -> None:
    """SPEC-0006: старый день с расхождением перекачивается, отметка не снимается окном."""
    from cluster_source.autoload import refresh_days

    repo = repos["GAZP"]
    old = (datetime.now(UTC) - timedelta(days=cfg.history_check_days + 40)).strftime("%Y-%m-%d")
    stale = (datetime.now(UTC) - timedelta(days=cfg.history_check_days + 41)).strftime("%Y-%m-%d")
    repo.mark_days_for_refresh([old], REFRESH_FINAL_MISMATCH)
    repo.mark_days_for_refresh([stale], "stream_gap")
    days = [d.strftime("%Y-%m-%d") for d in refresh_days(cfg, repo)]
    assert days == [old]
    assert repo.days_to_refresh() == {old: REFRESH_FINAL_MISMATCH}


async def test_archive_exhausted_stops_refresh(
    cfg: AppConfig, repos: Any, iss: IssTapeRepository, caplog: pytest.LogCaptureFixture
) -> None:
    """SPEC-0006 A-10 (G-5): перекачка не изменила день → archive_exhausted, без отметки."""
    d = date(2026, 10, 13)
    repo = repos["GAZP"]
    repo.insert_ticks(ticks("GAZP", d, 4))
    fake = FakeIss({("shares", d.isoformat()): {"GAZP": (5, 50.0)}})
    await check(cfg, repos, iss, fake)  # ночь 1: MISMATCH, отметка
    repo.clear_refresh_mark(d.isoformat())  # перекачка прошла, данные те же
    caplog.clear()
    with caplog.at_level(logging.ERROR):
        await check(cfg, repos, iss, fake)  # ночь 2
    df = get_day_verdicts(repo)
    assert status_of(repo, "2026-10-13") == STATUS_MISMATCH
    assert str(df["detail"].iloc[0]).startswith(ARCHIVE_EXHAUSTED)
    assert "DAY_ARCHIVE_EXHAUSTED GAZP 2026-10-13" in caplog.text
    assert "DAY_NOT_FINAL GAZP 2026-10-13" not in caplog.text
    assert repo.days_to_refresh() == {}
    assert get_baseline_days(repo) == []

    caplog.clear()  # ночь 3: молча, вердикт не переписан
    with caplog.at_level(logging.ERROR):
        stats = await check(cfg, repos, iss, fake)
    assert not stats and "GAZP" not in caplog.text
    assert list(get_day_verdicts(repo)["checked_at"]) == list(df["checked_at"])

    repo.insert_ticks(ticks("GAZP", d, 1))  # данные изменились (ручная перекачка)
    with caplog.at_level(logging.ERROR):
        await check(cfg, repos, iss, fake)
    assert status_of(repo, "2026-10-13") == STATUS_FINAL
    assert get_final_days(repo) == [d]


async def test_not_exhausted_while_refresh_pending(
    cfg: AppConfig, repos: Any, iss: IssTapeRepository, caplog: pytest.LogCaptureFixture
) -> None:
    """SPEC-0006 A-10 (G-5): перекачка не состоялась (отметка осталась) — не исчерпан."""
    d = date(2026, 10, 13)
    repo = repos["GAZP"]
    repo.insert_ticks(ticks("GAZP", d, 4))
    fake = FakeIss({("shares", d.isoformat()): {"GAZP": (5, 50.0)}})
    await check(cfg, repos, iss, fake)
    with caplog.at_level(logging.ERROR):
        await check(cfg, repos, iss, fake)  # отметка final_mismatch не снята
    assert "DAY_ARCHIVE_EXHAUSTED" not in caplog.text
    assert "DAY_NOT_FINAL GAZP 2026-10-13" in caplog.text
    assert not str(get_day_verdicts(repo)["detail"].iloc[0]).startswith(ARCHIVE_EXHAUSTED)
    assert repo.days_to_refresh() == {"2026-10-13": REFRESH_FINAL_MISMATCH}


async def test_settled_verdict_rechecked_when_day_data_changed(
    cfg: AppConfig, repos: Any, iss: IssTapeRepository, caplog: pytest.LogCaptureFixture
) -> None:
    """SPEC-0006 A-11 (G-6): данные дня (в т. ч. его выходных) изменились — вердикт
    пересчитывается; без изменений — не трогается."""
    fri, sun, mon = date(2026, 10, 9), date(2026, 10, 11), date(2026, 10, 12)
    repo = repos["GAZP"]
    repo.insert_ticks(ticks("GAZP", fri, 3) + ticks("GAZP", mon, 4))
    fake = FakeIss(
        {("shares", fri.isoformat()): {"GAZP": (3, 30.0)},
         ("shares", mon.isoformat()): {"GAZP": (6, 60.0)}}  # с воскресной сессией
    )  # fmt: skip
    await check(cfg, repos, iss, fake)
    assert status_of(repo, "2026-10-12") == STATUS_MISMATCH  # 4 из 6: нет воскресенья
    repo.set_day_verdict("2026-10-12", STATUS_FINAL, (4, 40.0), (4, 40.0), "iss_totals", "")
    stats = await check(cfg, repos, iss, fake)
    assert not stats  # FINAL с прежними данными не перепроверяется

    repo.insert_ticks(ticks("GAZP", sun, 2))  # перекачано воскресенье
    with caplog.at_level(logging.INFO):
        await check(cfg, repos, iss, fake)
    assert "DAY_DATA_CHANGED GAZP 2026-10-12: в вердикте 4 сделок, сейчас 6" in caplog.text
    row = get_day_verdicts(repo).set_index("trade_date").loc["2026-10-12"]
    assert (row["status"], row["ours_n"]) == (STATUS_FINAL, 6)
    assert "DAY_DATA_CHANGED GAZP 2026-10-09" not in caplog.text
