"""SPEC-0011: хвост вечерней сессии по свечам ISS."""

from __future__ import annotations

import asyncio
import logging
from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from typing import Any

import pandas as pd
import pytest
from test_final import FUTURES, FakeIss, cfg, iss, repos, status_of  # noqa: F401 - фикстуры

from cluster_source import main as main_mod
from cluster_source.autoload import BackfillResult, _settle_refresh_mark, days_to_load
from cluster_source.config import AppConfig, BatchConfig, IssConfig
from cluster_source.database import IssTapeRepository, MarketDataRepository
from cluster_source.final import (
    STATUS_ARCHIVE_CONSISTENT,
    STATUS_FINAL,
    STATUS_MISMATCH,
    run_final_check,
)
from cluster_source.reader import get_day_verdicts
from cluster_source.tail import (
    MINUTE_CANDLE_VERIFIED,
    MINUTE_GAP,
    TAIL_MISMATCH,
    TAIL_VERIFIED,
    compare_tail,
)

UTC = UTC
D = date(2026, 10, 13)
TODAY = date(2026, 10, 14)


def at(hhmmss: str) -> datetime:
    """Момент дня D по UTC (вечерняя сессия кончается в 20:50 UTC = 23:50 МСК)."""
    return datetime.fromisoformat(f"{D}T{hhmmss}+00:00")


def tick(name: str, moment: datetime, volume: int = 1, price: float = 100.0) -> dict[str, object]:
    return {
        "ticker": name,
        "timestamp": moment,
        "price": price,
        "volume": volume,
        "direction": "BUY",
        "trade_id": f"s{moment.timestamp()}",
    }


def tape_row(market: str, secid: str, board: str, no: int, moment: datetime) -> dict[str, object]:
    return {
        "market": market, "feed_date": D.isoformat(), "secid": secid, "board": board,
        "tradeno": no, "timestamp": moment.isoformat(timespec="microseconds"),
        "trade_date": D.isoformat(), "price": 100.0, "quantity": 1, "direction": "BUY",
        "period": None, "session": None, "open_position": None, "off_market": 0,
    }  # fmt: skip


HEAD = [at("20:00:00"), at("20:00:01"), at("20:00:02")]
CUTOFF = at("20:44:37")  # последняя сделка ленты ISS: 23:44:37 МСК
TAIL = [CUTOFF, at("20:46:10"), at("20:49:50")]
CANDLES = [  # минуты хвоста МСК: объём, максимум, минимум
    ("2026-10-13 23:44:00", 1.0, 100.0, 100.0),
    ("2026-10-13 23:46:00", 1.0, 100.0, 100.0),
    ("2026-10-13 23:49:00", 1.0, 100.0, 100.0),
]


def forts_day(repos: Any, iss: IssTapeRepository, extra_ours: list[datetime] = ()) -> None:  # noqa: F811
    """День Si: голова в ленте и у нас, хвост — только у нас (ISS его не публикует)."""
    repos["Si_CONTINUOUS"].insert_ticks(
        [tick("Si_CONTINUOUS", m) for m in [*HEAD, *TAIL, *extra_ours]]
    )
    iss.stage_rows(
        [tape_row("forts", "SiZ6", "RFUD", k + 1, m) for k, m in enumerate([*HEAD, CUTOFF])]
    )


TOTALS = {("forts", D.isoformat()): {"SiZ6": (6, 6.0)}}


async def check(cfg: AppConfig, repos: Any, iss: IssTapeRepository, fake: FakeIss,  # noqa: F811
                today: date = TODAY) -> Any:  # fmt: skip
    return await run_final_check(cfg, repos, iss, fake, FUTURES, today)


def verdicts(repo: MarketDataRepository) -> pd.DataFrame:
    return repo.read_minute_verdicts(at("20:00:00"), at("21:00:00"))


# --------------------------------------------------------------------------- #
def test_tail_minutes_compared_with_candles() -> None:
    """SPEC-0011 A-1: минуты хвоста — объём × лот, максимум, минимум против свечей."""
    start = at("20:44:00")
    ticks = pd.DataFrame(
        {
            "timestamp": pd.to_datetime([at("20:43:59"), at("20:44:10"), at("20:44:50"),
                                         at("20:45:30")], utc=True),
            "price": [99.0, 100.0, 101.0, 100.0],
            "volume": [5, 2, 1, 4],
        }
    )  # fmt: skip
    candles = {
        at("20:44:00"): (30.0, 101.0, 100.0),  # 3 лота × 10 — совпала
        at("20:45:00"): (40.0, 100.5, 100.0),  # максимум другой
        at("20:46:00"): (10.0, 100.0, 100.0),  # у нас сделок нет
    }
    tail = compare_tail(ticks, candles, start, lot=10)
    assert tail.status == TAIL_MISMATCH
    assert (tail.n, tail.volume) == (3, 7.0)  # сделка 20:43:59 — голова
    assert [(m[0][11:16], m[1]) for m in tail.minutes] == [
        ("20:44", MINUTE_CANDLE_VERIFIED),
        ("20:45", MINUTE_GAP),
        ("20:46", MINUTE_GAP),
    ]
    assert tail.bad == ["23:45", "23:46"]
    assert "candle_mismatch" in tail.minutes[1][5]
    ok = compare_tail(ticks, {at("20:44:00"): (30.0, 101.0, 100.0),
                              at("20:45:00"): (40.0, 100.0, 100.0)}, start, lot=10)  # fmt: skip
    assert ok.status == TAIL_VERIFIED


async def test_forts_final_with_candle_tail(
    cfg: AppConfig,  # noqa: F811
    repos: Any,  # noqa: F811
    iss: IssTapeRepository,  # noqa: F811
    caplog: pytest.LogCaptureFixture,
) -> None:
    """SPEC-0011 A-2 (G-1): голова = лента, хвост = свечи → FINAL с пометкой «хвост по свечам»."""
    forts_day(repos, iss)
    with caplog.at_level(logging.INFO):
        await check(cfg, repos, iss, FakeIss(TOTALS, {"SiZ6": CANDLES}))
    repo = repos["Si_CONTINUOUS"]
    row = get_day_verdicts(repo).iloc[0]
    assert (row["status"], row["reference"]) == (STATUS_FINAL, "iss_tape")
    assert (row["ours_n"], row["ref_n"]) == (6, 6)
    assert "хвост по свечам с 23:44 МСК: 3 сделок / 3" in row["detail"]
    assert "TAIL Si_CONTINUOUS 2026-10-13: хвост с 23:44 МСК, минут 3" in caplog.text
    mv = verdicts(repo)
    assert list(mv["status"]) == [MINUTE_CANDLE_VERIFIED] * 3


@pytest.mark.parametrize("broken", ["head", "tail"])
async def test_forts_head_or_tail_mismatch(
    cfg: AppConfig,  # noqa: F811
    repos: Any,  # noqa: F811
    iss: IssTapeRepository,  # noqa: F811
    caplog: pytest.LogCaptureFixture,
    broken: str,
) -> None:
    """SPEC-0011 A-3: голова не совпала с лентой или хвост со свечами → MISMATCH."""
    candles = list(CANDLES)
    if broken == "head":
        forts_day(repos, iss, extra_ours=[at("20:10:00")])  # сделки нет в ленте
    else:
        forts_day(repos, iss)
        candles[1] = ("2026-10-13 23:46:00", 7.0, 100.0, 100.0)  # у нас в минуте меньше
    with caplog.at_level(logging.ERROR):
        await check(cfg, repos, iss, FakeIss(TOTALS, {"SiZ6": candles}))
    row = get_day_verdicts(repos["Si_CONTINUOUS"]).iloc[0]
    assert row["status"] == STATUS_MISMATCH
    if broken == "head":
        assert "до среза у нас 4 сделок, в ленте 3" in row["detail"]
    else:
        assert "TAIL_MISMATCH Si_CONTINUOUS 2026-10-13: минуты 23:46" in caplog.text
        assert MINUTE_GAP in set(verdicts(repos["Si_CONTINUOUS"])["status"])


async def test_long_tail_is_not_publication_gap(
    cfg: AppConfig,  # noqa: F811
    repos: Any,  # noqa: F811
    iss: IssTapeRepository,  # noqa: F811
) -> None:
    """SPEC-0011 A-4 (G-2): хвост длиннее 20 минут — лента не покрывает день."""
    repo = repos["Si_CONTINUOUS"]
    repo.insert_ticks([tick("Si_CONTINUOUS", m) for m in [*HEAD, *TAIL]])
    iss.stage_rows([tape_row("forts", "SiZ6", "RFUD", k + 1, m) for k, m in enumerate(HEAD)])
    fake = FakeIss(TOTALS, {"SiZ6": CANDLES})
    await check(cfg, repos, iss, fake)
    row = get_day_verdicts(repo).iloc[0]
    assert (row["status"], row["reference"]) == (STATUS_ARCHIVE_CONSISTENT, "iss_totals")
    assert verdicts(repo).empty  # свечами не подтверждали


async def test_tail_unavailable_retried(
    cfg: AppConfig,  # noqa: F811
    repos: Any,  # noqa: F811
    iss: IssTapeRepository,  # noqa: F811
    caplog: pytest.LogCaptureFixture,
) -> None:
    """SPEC-0011 A-5 (G-3): свечей нет — без вердикта; через 3 суток — как без ленты."""
    forts_day(repos, iss)
    fake = FakeIss(TOTALS)  # свечей нет
    with caplog.at_level(logging.WARNING):
        await check(cfg, repos, iss, fake)
    assert get_day_verdicts(repos["Si_CONTINUOUS"]).empty
    assert "TAIL_UNAVAILABLE Si_CONTINUOUS 2026-10-13" in caplog.text
    await check(cfg, repos, iss, fake, today=D + timedelta(days=3))
    assert status_of(repos["Si_CONTINUOUS"], "2026-10-13") == STATUS_ARCHIVE_CONSISTENT


async def test_recent_archive_consistent_rechecked(
    cfg: AppConfig,  # noqa: F811
    repos: Any,  # noqa: F811
    iss: IssTapeRepository,  # noqa: F811
) -> None:
    """SPEC-0011 A-6: ARCHIVE_CONSISTENT моложе 3 суток перепроверяется, старше — нет."""
    repo = repos["Si_CONTINUOUS"]
    forts_day(repos, iss)
    old = date(2026, 10, 9)
    repo.insert_ticks([tick("Si_CONTINUOUS", datetime(2026, 10, 9, 10, tzinfo=UTC))])
    for day in (D, old):
        repo.set_day_verdict(day.isoformat(), STATUS_ARCHIVE_CONSISTENT, (1, 1.0), (1, 1.0),
                             "iss_totals", "")  # fmt: skip
    totals = {**TOTALS, ("forts", old.isoformat()): {"SiZ6": (1, 1.0)}}
    await check(cfg, repos, iss, FakeIss(totals, {"SiZ6": CANDLES}))
    assert status_of(repo, D.isoformat()) == STATUS_FINAL
    df = get_day_verdicts(repo)
    old_row = df.loc[df["trade_date"] == old.isoformat()].iloc[0]
    assert (old_row["status"], old_row["checked_at"] < df["checked_at"].max()) == (
        STATUS_ARCHIVE_CONSISTENT,
        True,
    )


async def test_shares_tail_minutes_recorded(
    cfg: AppConfig,  # noqa: F811
    repos: Any,  # noqa: F811
    iss: IssTapeRepository,  # noqa: F811
) -> None:
    """SPEC-0011 A-7: акции — хвост даёт минутные вердикты, статус дня — по итогам."""
    repo = repos["GAZP"]
    repo.insert_ticks([tick("GAZP", m, volume=2) for m in [*HEAD, *TAIL]])
    iss.stage_rows(
        [tape_row("shares", "GAZP", "TQBR", k + 1, m) for k, m in enumerate([*HEAD, CUTOFF])]
    )
    candles = [(begin, 20.0, hi, lo) for begin, _v, hi, lo in CANDLES]  # 2 лота × 10 штук
    totals = {("shares", D.isoformat()): {"GAZP": (6, 120.0)}}
    await check(cfg, repos, iss, FakeIss(totals, {"GAZP": candles}))
    row = get_day_verdicts(repo).iloc[0]
    assert (row["status"], row["reference"]) == (STATUS_FINAL, "iss_totals")
    assert "хвост по свечам с 23:44 МСК: 3 сделок / 6" in row["detail"]
    assert list(verdicts(repo)["status"]) == [MINUTE_CANDLE_VERIFIED] * 3


# --------------------------------------------------------------------------- #
def test_startup_maintenance_skips_today(cfg: AppConfig, repos: Any) -> None:  # noqa: F811
    """SPEC-0011 A-9 (G-5): стартовое обслуживание не дописывает текущий день."""
    repo = repos["GAZP"]
    today = datetime.now(UTC).replace(hour=0, minute=0, second=0, microsecond=0)
    missing, _ = days_to_load(cfg, repo)
    assert today in missing
    missing, _ = days_to_load(cfg, repo, include_today=False)
    assert today not in missing and today - timedelta(days=1) in missing


async def test_stream_opens_before_startup_maintenance(
    cfg: AppConfig,  # noqa: F811
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """SPEC-0011 A-9 (G-5): стрим открывается до стартового обслуживания, текущий день
    обслуживание не трогает."""
    events: list[str] = []

    class FakeHub:
        tickers: dict[str, str] = {}

        def __init__(self, *_: Any) -> None:
            pass

        async def run(self) -> None:
            events.append("stream")
            await asyncio.Event().wait()

        async def shutdown(self) -> None:
            pass

        def request_reconnect(self, resolve: bool = False) -> None:
            pass

    async def fake_maintenance(*_: Any, include_today: bool = True, **__: Any) -> None:
        events.append(f"maintenance include_today={include_today}")

    async def idle(*_: Any, **__: Any) -> None:
        await asyncio.Event().wait()

    monkeypatch.setattr(main_mod, "StreamHub", FakeHub)
    monkeypatch.setattr(main_mod, "make_connection_factory", lambda _c: None)
    monkeypatch.setattr(main_mod, "run_maintenance", fake_maintenance)
    monkeypatch.setattr(main_mod, "run_scheduler", idle)
    monkeypatch.setattr(main_mod, "run_dealer_cleanup", idle)
    config = replace(cfg, iss=IssConfig(enabled=False), batch=BatchConfig(flush_interval_sec=0.05))
    with pytest.raises(TimeoutError):
        await asyncio.wait_for(main_mod.amain(config), timeout=0.5)
    assert events == ["stream", "maintenance include_today=False"]


def test_unpublished_archive_message(
    cfg: AppConfig,  # noqa: F811
    repos: Any,  # noqa: F811
    caplog: pytest.LogCaptureFixture,
) -> None:
    """SPEC-0011 A-10: архив за вчера ещё не опубликован — так и пишем, отметка сохранена."""
    repo = repos["GAZP"]
    day = datetime.now(UTC).replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=1)
    day_str = day.strftime("%Y-%m-%d")
    repo.insert_ticks([tick("GAZP", day + timedelta(hours=10))])
    repo.mark_days_for_refresh([day_str], "stream_gap")
    with caplog.at_level(logging.INFO):
        _settle_refresh_mark(repo, day, BackfillResult.NO_DATA)
    assert f"Архив истории за {day_str} для GAZP ещё не опубликован — отметка сохранена" in (
        caplog.text
    )
    assert day_str in repo.days_to_refresh()
