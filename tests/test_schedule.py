"""SPEC-0012: расписание торгов — календарь торговых дней ISS."""

from __future__ import annotations

import logging
from datetime import date
from typing import Any

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
