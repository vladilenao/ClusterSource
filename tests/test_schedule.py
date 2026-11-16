"""SPEC-0012: расписание торгов — календарь торговых дней ISS."""

from __future__ import annotations

import logging
from datetime import date
from typing import Any

import pandas as pd
import pytest

from cluster_source.config import AppConfig
from cluster_source.database import IssTapeRepository
from cluster_source.schedule import is_trading_day, load_iss_calendar


class FakeIss:
    """Ответы ``/iss/engines/{stock,futures}.json``; ``fail`` — ISS недоступен."""

    def __init__(self, daily: list[list[object]]) -> None:
        self.daily = daily
        self.fail: set[str] = set()

    async def get_json(self, url: str, params: dict[str, str]) -> dict[str, Any]:
        engine = "stock" if url.endswith("/stock.json") else "futures"
        if engine in self.fail:
            raise ConnectionError("ISS недоступен")
        assert params["iss.only"] == "timetable,dailytable"
        return {
            "timetable": {
                "columns": ["week_day", "is_work_day", "start_time", "stop_time"],
                "data": [[k, 1, "06:20:00", "23:59:59"] for k in range(1, 8)],
            },
            "dailytable": {
                "columns": ["date", "is_work_day", "start_time", "stop_time"],
                "data": self.daily,
            },
        }


DAILY = [
    ["2026-01-01", 0, "06:00:00", "23:59:59"],  # праздник
    ["2026-09-12", 0, "06:00:00", "23:59:59"],  # отменённая сессия выходного дня (суббота)
    ["2026-01-05", 1, "06:00:00", "23:59:59"],  # торговый день в праздники
]


@pytest.fixture
def repo(share_config: AppConfig) -> IssTapeRepository:
    return IssTapeRepository(share_config.storage_root)


async def test_iss_calendar_loaded_and_kept(repo: IssTapeRepository) -> None:
    """SPEC-0012 A-1 (G-1): календарь обоих рынков по датам; повтор не теряет даты."""
    assert await load_iss_calendar(FakeIss(DAILY), repo) == {"shares": 3, "forts": 3}
    assert repo.calendar_span("shares") == ("2026-01-01", "2026-09-12", 3)
    await load_iss_calendar(FakeIss([["2026-10-20", 0, None, None]]), repo)
    assert repo.calendar_span("forts") == ("2026-01-01", "2026-10-20", 4)  # старые даты на месте


async def test_is_trading_day_rule(repo: IssTapeRepository) -> None:
    """SPEC-0012 A-2: исключение важнее недельного шаблона; без календаря — None."""
    assert is_trading_day(repo, "shares", date(2026, 9, 12)) is None
    await load_iss_calendar(FakeIss(DAILY), repo)
    assert is_trading_day(repo, "shares", date(2026, 9, 12)) is False  # суббота, отменена
    assert is_trading_day(repo, "shares", date(2026, 9, 19)) is True  # суббота по шаблону
    assert is_trading_day(repo, "forts", date(2026, 1, 1)) is False
    assert is_trading_day(repo, "forts", date(2026, 1, 5)) is True


async def test_calendar_load_failure_keeps_previous(
    repo: IssTapeRepository, caplog: pytest.LogCaptureFixture
) -> None:
    """SPEC-0012 A-3 (G-2): ошибка ISS не роняет загрузку, прежний календарь остаётся."""
    await load_iss_calendar(FakeIss(DAILY), repo)
    broken = FakeIss([])
    broken.fail = {"stock"}
    with caplog.at_level(logging.WARNING):
        loaded = await load_iss_calendar(broken, repo)
    assert "shares" not in loaded
    assert "Ошибка загрузки календаря ISS shares" in caplog.text
    assert repo.calendar_span("shares") == ("2026-01-01", "2026-09-12", 3)


# --------------------------------------------------------------------------- #
# Этап 2: расписание сессий и режимы (SPEC-0012 G-3)
# --------------------------------------------------------------------------- #
from dataclasses import replace  # noqa: E402
from datetime import UTC, datetime, timedelta  # noqa: E402
from types import SimpleNamespace  # noqa: E402

from cluster_source.config import Instrument, InstrumentType, SignalsConfig  # noqa: E402
from cluster_source.database import MarketDataRepository  # noqa: E402
from cluster_source.iss import MSK  # noqa: E402
from cluster_source.schedule import (  # noqa: E402
    Regimes,
    check_iss_session_schedule,
    intervals_from_days,
    kind_to_regime,
    load_tinvest_schedule,
)
from cluster_source.signals.baseline import compute_norms  # noqa: E402


def msk(day: str, hhmm: str) -> datetime:
    return datetime.fromisoformat(f"{day}T{hhmm}").replace(tzinfo=MSK)


def interval(kind: str, day: str, start: str, end: str) -> SimpleNamespace:
    return SimpleNamespace(
        type=kind, interval=SimpleNamespace(start_ts=msk(day, start), end_ts=msk(day, end))
    )


WEEKDAY = "2026-10-15"
SHARES_DAY = SimpleNamespace(
    date=datetime(2026, 10, 15, tzinfo=UTC),
    intervals=[
        interval("opening_auction_morning", WEEKDAY, "06:50", "06:59"),
        interval("opening_auction", WEEKDAY, "06:50", "06:59"),
        interval("regular_trading_session_morning", WEEKDAY, "07:00", "08:59"),
        interval("regular_trading_session", WEEKDAY, "07:00", "08:59"),
        interval("regular_trading_session_main", WEEKDAY, "09:00", "18:54"),
        interval("closing_auction_main", WEEKDAY, "18:55", "18:59"),
        interval("regular_trading_session_evening", WEEKDAY, "19:00", "23:49"),
        interval("dealer_regular_trading_session", WEEKDAY, "02:00", "23:50"),
    ],
)


def test_intervals_to_regimes() -> None:
    """SPEC-0012 A-4: дилерские и дубли отброшены, аукционы, выходные, клиринг — пауза."""
    assert kind_to_regime("dealer_holiday_trading_session_morning") == (False, None)
    assert kind_to_regime("regular_trading_session") == (False, None)
    assert kind_to_regime("holiday_opening_auction_morning") == (True, "auction")
    assert kind_to_regime("closing_auction_main") == (True, "auction")
    assert kind_to_regime("holiday_trading_session_morning") == (True, "weekend")
    assert kind_to_regime("regular_trading_session_main") == (True, "main")
    assert kind_to_regime("clearing") == (True, None)
    rows = intervals_from_days([SHARES_DAY])[WEEKDAY]
    assert [r[1] for r in rows] == ["auction", "morning", "main", "auction", "evening"]


async def test_schedule_saved_and_checked_against_iss(
    repo: IssTapeRepository, caplog: pytest.LogCaptureFixture
) -> None:
    """SPEC-0012 A-5 (G-2): расписание по дням, повтор заменяет день; сверка с ISS."""
    forts_day = SimpleNamespace(
        date=datetime(2026, 10, 15, tzinfo=UTC),
        intervals=[
            interval("regular_trading_session_morning", WEEKDAY, "07:00", "08:59"),
            interval("regular_trading_session_main", WEEKDAY, "09:00", "18:59"),
            interval("regular_trading_session_evening", WEEKDAY, "19:05", "23:50"),
            interval("clearing", WEEKDAY, "23:50", "23:59"),
        ],
    )

    class Instruments:
        def __init__(self) -> None:
            self.calls: list[str] = []

        async def trading_schedules(self, exchange: str, from_: Any, to: Any) -> Any:
            self.calls.append(exchange)
            if exchange == "broken":
                raise ConnectionError("нет связи")
            day = SHARES_DAY if exchange.startswith("moex") else forts_day
            return SimpleNamespace(exchanges=[SimpleNamespace(days=[day])])

    service = Instruments()
    loaded = await load_tinvest_schedule(
        service, repo, {"shares": "moex_x", "forts": "forts_x"}, date(2026, 10, 15)
    )
    assert loaded == {"shares": 1, "forts": 1}
    assert len(repo.sessions("shares", WEEKDAY)) == 5
    assert repo.session_end("shares", WEEKDAY) == msk(WEEKDAY, "23:49")
    assert repo.session_end("forts", WEEKDAY) == msk(WEEKDAY, "23:50")
    await load_tinvest_schedule(service, repo, {"shares": "broken"}, date(2026, 10, 15))
    assert len(repo.sessions("shares", WEEKDAY)) == 5  # прежнее осталось

    class Iss:
        async def get_json(self, url: str, params: dict[str, str]) -> dict[str, Any]:
            columns = ["trade_session_date", "boardid", "secid", "type", "time_from", "time_till"]
            day = WEEKDAY

            def row(kind: str, start: str, end: str | None) -> list[object]:
                return [day, "-", "-", kind, f"{day} {start}:00", end and f"{day} {end}:00"]

            data = [
                row("morning_session", "07:00", "09:00"),
                row("main_session", "09:00", "19:00"),
                row("evening_session", "19:00", "23:50"),
                row("clearing_session", "23:50", None),
            ]
            return {"session_schedule": {"columns": columns, "data": data}}

    with caplog.at_level(logging.WARNING):
        mismatches = await check_iss_session_schedule(Iss(), repo, date(2026, 10, 15))
    assert mismatches == ["сессия evening — T-Invest 19:05, ISS 19:00"]
    assert "SCHEDULE_MISMATCH forts 2026-10-15" in caplog.text
    assert len(repo.sessions("forts", WEEKDAY, "iss")) == 3


async def test_regimes_from_schedule_with_fallback(repo: IssTapeRepository) -> None:
    """SPEC-0012 A-6 (G-3): режим — по расписанию дня, нет дня — по конфигурации."""
    rows = intervals_from_days([SHARES_DAY])[WEEKDAY]
    repo.replace_sessions("shares", WEEKDAY, "tinvest", "moex_x", rows, "now")
    regimes = Regimes(SignalsConfig(), repo)
    assert regimes.regime(msk(WEEKDAY, "06:55"), "shares") == "auction"
    assert regimes.regime(msk(WEEKDAY, "08:59:59"), "shares") == "morning"  # конец — включительно
    assert regimes.regime(msk(WEEKDAY, "18:57"), "shares") == "auction"
    assert regimes.regime(msk(WEEKDAY, "23:55"), "shares") is None
    other = "2026-10-14"  # расписания нет — границы из конфигурации
    assert regimes.regime(msk(other, "06:55"), "shares") == "morning"
    assert regimes.regime(msk(other, "18:57"), "shares") == "main"
    stamps = pd.Series(
        [msk(WEEKDAY, "06:55"), msk(WEEKDAY, "12:00"), msk(other, "06:55")]
    ).dt.tz_convert(UTC)
    assert list(regimes.labels(stamps, "shares")) == ["auction", "main", "morning"]


async def test_norms_use_schedule_regimes(share_config: AppConfig) -> None:
    """SPEC-0012 A-6: нормы считают аукционы отдельным режимом."""
    sber = Instrument(id="SBER", type=InstrumentType.SHARE, tick_size=0.01, lot_size=1)
    repo = MarketDataRepository(share_config.storage_root, sber)
    iss = IssTapeRepository(share_config.storage_root)
    days = [date(2026, 10, d) for d in (5, 6, 7, 8, 9)]
    for day in days:
        iso = day.isoformat()
        day_rows = [
            (k, r, s.replace(year=day.year, month=day.month, day=day.day),
             e.replace(year=day.year, month=day.month, day=day.day))
            for k, r, s, e in intervals_from_days([SHARES_DAY])[WEEKDAY]
        ]  # fmt: skip
        iss.replace_sessions("shares", iso, "tinvest", "moex_x", day_rows, "now")
        auction = msk(iso, "06:59")
        repo.insert_ticks(
            [{"ticker": "SBER", "timestamp": auction + timedelta(milliseconds=i), "price": 300.0,
              "volume": 5000, "direction": "BUY" if i % 2 else "SELL", "trade_id": f"a{i}"}
             for i in range(60)]
            + [{"ticker": "SBER", "timestamp": msk(iso, "12:00") + timedelta(seconds=i),
                "price": 300.0, "volume": 1, "direction": "BUY" if i % 2 else "SELL",
                "trade_id": f"m{i}"} for i in range(60)]
        )  # fmt: skip
        repo.set_day_verdict(iso, "FINAL", (1, 1.0), (1, 1.0), "iss_totals", "")
    cfg = replace(SignalsConfig(), large_aggressor_percentile=50.0)
    norms = compute_norms(repo, sber, cfg, Regimes(cfg, iss))
    assert norms is not None
    assert set(norms.aggressor_threshold) == {"auction", "main"}
    assert norms.aggressor_threshold["auction"] > norms.aggressor_threshold["main"]
