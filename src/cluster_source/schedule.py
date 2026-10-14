"""Расписание торгов (SPEC-0012), этап 1 — календарь торговых дней ISS.

Ни T-Invest, ни ISS не отдают историю времени сессий, зато ISS отдаёт
календарь рынков (``/iss/engines/{stock,futures}.json``): недельный шаблон
``timetable`` (все 7 дней торговые — с сессиями выходного дня) и исключения по
датам ``dailytable`` с 2018 года — праздники, отменённые сессии выходного дня,
рабочие дни в праздники. Времена в нём — часы торговой системы, не сессий.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import UTC, date, datetime

from cluster_source.database import IssTapeRepository
from cluster_source.iss import ISS_BASE, IssClient

logger = logging.getLogger(__name__)

ENGINE_URLS: dict[str, str] = {
    "shares": f"{ISS_BASE}/engines/stock.json",
    "forts": f"{ISS_BASE}/engines/futures.json",
}


def _rows(data: dict[str, object], block: str) -> list[dict[str, object]]:
    raw = data.get(block)
    if not isinstance(raw, dict):
        return []
    columns = list(raw.get("columns") or [])
    return [dict(zip(columns, row, strict=False)) for row in raw.get("data") or []]


def _opt(value: object) -> str | None:
    return None if value in (None, "") else str(value)


async def load_iss_calendar(client: IssClient, repo: IssTapeRepository) -> dict[str, int]:
    """Загружает календарь обоих рынков; ``{рынок: число дат-исключений}``.

    Ошибка одного рынка логируется, прежний календарь остаётся (SPEC-0012 G-2).
    """
    loaded: dict[str, int] = {}
    now = datetime.now(UTC).isoformat()
    for market, url in ENGINE_URLS.items():
        try:
            data = await client.get_json(
                url, {"iss.meta": "off", "iss.only": "timetable,dailytable"}
            )
            days = [
                (str(r["date"]), int(str(r["is_work_day"])), _opt(r.get("start_time")),
                 _opt(r.get("stop_time")))
                for r in _rows(data, "dailytable")
            ]  # fmt: skip
            week = [
                (int(str(r["week_day"])), int(str(r["is_work_day"])), _opt(r.get("start_time")),
                 _opt(r.get("stop_time")))
                for r in _rows(data, "timetable")
            ]  # fmt: skip
            if not days and not week:
                raise ValueError("пустой ответ")
            await asyncio.to_thread(repo.save_calendar, market, days, week, now)
        except Exception as exc:  # noqa: BLE001 - календарь не должен ронять обслуживание
            logger.warning("Ошибка загрузки календаря ISS %s: %s", market, exc)
            continue
        off = sum(1 for _, work, _, _ in days if not work)
        logger.info(
            "ISS_CALENDAR %s: дат %d (неторговых %d, торговых-исключений %d), с %s",
            market,
            len(days),
            off,
            len(days) - off,
            min((d for d, *_ in days), default="—"),
        )
        loaded[market] = len(days)
    return loaded


def is_trading_day(repo: IssTapeRepository, market: str, day: date) -> bool | None:
    """Торговый ли день рынка по календарю ISS; ``None`` — календарь не загружен.

    Исключение ``dailytable`` важнее недельного шаблона ``timetable``.
    """
    override = repo.calendar_day(market, day.isoformat())
    if override is not None:
        return bool(override)
    week = repo.week_timetable(market)
    if not week:
        return None
    work = week.get(day.isoweekday())
    return None if work is None else bool(work)
