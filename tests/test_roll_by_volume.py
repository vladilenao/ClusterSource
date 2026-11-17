"""SPEC-0014: ролл по объёму — правило выбора контракта, решения, перевод истории."""

from __future__ import annotations

import logging
from datetime import UTC, date, datetime
from types import SimpleNamespace
from typing import Any

import pytest

from cluster_source import main as main_mod
from cluster_source import roll
from cluster_source.autoload import REFRESH_ROLL_RULE, archive_key_candidates, current_figi
from cluster_source.config import AppConfig, FuturesMeta, Instrument, InstrumentType
from cluster_source.contracts import contract_of
from cluster_source.database import IssTapeRepository, MarketDataRepository
from cluster_source.iss import KIND_FEED, STATUS_INCOMPLETE, normalize_trades
from cluster_source.reader import get_contract

SI = Instrument(id="Si", type=InstrumentType.CONTINUOUS_FUTURES, tick_size=1, lot_size=1)
U6 = FuturesMeta("SiU6", "SPBFUT", date(2026, 9, 18), "FU6", last_trade=date(2026, 9, 17))
Z6 = FuturesMeta("SiZ6", "SPBFUT", date(2026, 12, 18), "FZ6", last_trade=date(2026, 12, 17))
H7 = FuturesMeta("SiH7", "SPBFUT", date(2027, 3, 19), "FH7", last_trade=date(2027, 3, 18))
M7 = FuturesMeta("SiM7", "SPBFUT", date(2027, 6, 18), "FM7", last_trade=date(2027, 6, 17))
FUTURES = {m.ticker: m for m in (U6, Z6, H7, M7)}
CONTRACTS = roll.contracts_of(SI, FUTURES.values())
NOW = datetime(2026, 12, 15, 0, 5, tzinfo=UTC)
#: P и P−1 для 15.12 (вторник).
PP = [date(2026, 12, 14), date(2026, 12, 11)]
FORTS_COLUMNS = [
    "TRADENO", "BOARDNAME", "SECID", "TRADEDATE", "TRADETIME", "PRICE", "QUANTITY", "SYSTIME",
    "RECNO", "OPENPOSITION", "OFFMARKETDEAL", "BUYSELL", "TRADE_SESSION_DATE",
]  # fmt: skip


def fixed(volumes: dict[str, float] | None, source: str = "лента") -> roll.VolumeSource:
    def source_fn(_day: date, secids: Any) -> tuple[dict[str, float], str] | None:
        if volumes is None:
            return None
        return {s: volumes.get(s, 0.0) for s in secids}, source

    return source_fn


def by_day(table: dict[date, dict[str, float]]) -> roll.VolumeSource:
    def source_fn(day: date, secids: Any) -> tuple[dict[str, float], str] | None:
        if day not in table:
            return None
        return {s: table[day].get(s, 0.0) for s in secids}, "итоги"

    return source_fn


def tape(iss: IssTapeRepository, day: str, trades: list[tuple[str, int]]) -> None:
    """Лента FORTS торгового дня ``day``: ``[(secid, qty)]``."""
    base = int(day.replace("-", "")) * 100  # номера сделок разных дней не совпадают
    rows = [
        [
            base + n + 1,
            "RFUD",
            secid,
            day,
            "12:00:00",
            80000,
            qty,
            f"{day} 12:00:00",
            base + n + 1,
            0,
            0,
            "B",
            day,
        ]
        for n, (secid, qty) in enumerate(trades)
    ]
    iss.stage_rows(normalize_trades("forts", day, FORTS_COLUMNS, rows))


@pytest.fixture
def repo(share_config: AppConfig) -> MarketDataRepository:
    return MarketDataRepository(share_config.storage_root, SI)


@pytest.fixture
def iss(share_config: AppConfig) -> IssTapeRepository:
    return IssTapeRepository(share_config.storage_root)


# --------------------------------------------------------------------------- #
def test_roll_rule_steps(iss: IssTapeRepository) -> None:
    """SPEC-0014 A-1, G-2: шаги expired / last_day / volume / carry; P — будний торговый день."""
    d = roll.decide(SI, CONTRACTS, date(2026, 9, 18), "SiU6", PP, fixed({}))
    assert d == roll.Decision("SiZ6", roll.REASON_EXPIRED, "SiU6 истёк")

    d = roll.decide(SI, CONTRACTS, date(2026, 12, 17), "SiZ6", PP, fixed({}))
    assert d is not None and (d.ticker, d.reason) == ("SiH7", roll.REASON_LAST_DAY)

    up = fixed({"SiZ6": 410_220, "SiH7": 523_104})
    d = roll.decide(SI, CONTRACTS, date(2026, 12, 15), "SiZ6", PP, up)
    assert d is not None and (d.ticker, d.reason) == ("SiH7", roll.REASON_VOLUME)
    assert d.basis == (
        "SiH7 523 104 > SiZ6 410 220 за 2026-12-14, лента;"
        " SiH7 523 104 > SiZ6 410 220 за 2026-12-11, лента"
    )

    # Перевес один день (P) — шум, не переход (ролл Eu 07.09).
    once = by_day(
        {PP[0]: {"SiZ6": 95_560, "SiH7": 97_109}, PP[1]: {"SiZ6": 94_911, "SiH7": 27_608}}
    )
    d = roll.decide(SI, CONTRACTS, date(2026, 12, 15), "SiZ6", PP, once)
    assert d is not None and (d.ticker, d.reason) == ("SiZ6", roll.REASON_CARRY)
    assert d.basis is not None and d.basis.endswith(
        "SiH7 27 608 <= SiZ6 94 911 за 2026-12-11, итоги"
    )

    down = fixed({"SiZ6": 523_104, "SiH7": 410_220})
    d = roll.decide(SI, CONTRACTS, date(2026, 12, 15), "SiZ6", PP, down)
    assert d is not None and (d.ticker, d.reason) == ("SiZ6", roll.REASON_CARRY)
    d = roll.decide(SI, CONTRACTS, date(2026, 12, 15), None, PP, down)
    assert d is not None and (d.ticker, d.reason) == ("SiZ6", roll.REASON_INITIAL)

    # Выходные относятся к понедельнику, P — пятница; праздник по календарю ISS пропускается.
    assert roll.trading_day_of(iss, date(2026, 12, 12)) == date(2026, 12, 14)
    assert roll.prior_trading_days(iss, date(2026, 12, 14)) == [
        date(2026, 12, 11),
        date(2026, 12, 10),
    ]
    iss.save_calendar("forts", [("2026-11-04", 0, None, None)], [], NOW.isoformat())
    assert roll.trading_day_of(iss, date(2026, 11, 4)) == date(2026, 11, 5)
    assert roll.prior_trading_days(iss, date(2026, 11, 5)) == [date(2026, 11, 3), date(2026, 11, 2)]


def test_roll_rule_last_day_covers_weekend(
    repo: MarketDataRepository, iss: IssTapeRepository
) -> None:
    """SPEC-0014 A-1: сессии выходных перед последним днём торгов — уже у следующего контракта."""
    meta = FuturesMeta("SiZ6", "SPBFUT", date(2026, 12, 15), "FZ6", last_trade=date(2026, 12, 14))
    futures = {**FUTURES, "SiZ6": meta}
    repo.save_active_contract("2026-12-11", "SiZ6", "carry", None, NOW.isoformat())
    got = roll.ensure_decisions(
        SI, repo, iss, futures.values(), [date(2026, 12, 12), date(2026, 12, 14)], fixed({})
    )
    assert got == {date(2026, 12, 12): "SiH7", date(2026, 12, 14): "SiH7"}


def test_roll_never_back_nor_third() -> None:
    """SPEC-0014 A-2, G-2: со второго ближайшего не возвращается, на третий не уходит."""
    back = fixed({"SiZ6": 900_000, "SiH7": 1_000})
    d = roll.decide(SI, CONTRACTS, date(2026, 12, 15), "SiH7", PP, back)
    assert d is not None and (d.ticker, d.reason) == ("SiH7", roll.REASON_CARRY)

    third = fixed({"SiZ6": 10_000, "SiH7": 1_000, "SiM7": 99_000_000})
    d = roll.decide(SI, CONTRACTS, date(2026, 12, 15), "SiZ6", PP, third)
    assert d is not None and d.ticker == "SiZ6"


def test_roll_volume_source(iss: IssTapeRepository, caplog: pytest.LogCaptureFixture) -> None:
    """SPEC-0014 A-3: лента, при ISS_FEED_INCOMPLETE — итоги, без данных — ROLL_NO_VOLUME."""
    day = date(2026, 12, 14)
    tape(iss, "2026-12-14", [("SiZ6", 5), ("SiH7", 7), ("SiZ6SiH7", 900)])
    volumes = roll.LocalVolumes(iss)
    assert volumes(day, ["SiZ6", "SiH7"]) == ({"SiZ6": 5.0, "SiH7": 7.0}, "лента")

    iss.set_day_status("forts", "2026-12-14", KIND_FEED, STATUS_INCOMPLETE, "")
    iss.save_totals("forts", "2026-12-14", {"SiZ6": (10, 200.0), "SiH7": (5, 300.0)})
    assert roll.LocalVolumes(iss)(day, ["SiZ6", "SiH7"]) == (
        {"SiZ6": 200.0, "SiH7": 300.0},
        "итоги",
    )

    assert roll.LocalVolumes(iss)(date(2026, 12, 11), ["SiZ6", "SiH7"]) is None
    caplog.set_level(logging.WARNING)
    d = roll.decide(SI, CONTRACTS, date(2026, 12, 15), "SiZ6", PP, volumes)
    assert d is not None and (d.ticker, d.reason) == ("SiZ6", roll.REASON_CARRY)
    assert "ROLL_NO_VOLUME Si_CONTINUOUS 2026-12-15: нет объёмов за 2026-12-11" in caplog.text


def test_roll_decision_frozen(repo: MarketDataRepository, iss: IssTapeRepository) -> None:
    """SPEC-0014 A-4, G-1: решение пишется один раз; другие объёмы его не меняют."""
    day = date(2026, 12, 15)
    repo.save_active_contract("2026-12-14", "SiZ6", "carry", None, NOW.isoformat())
    first = roll.ensure_decisions(SI, repo, iss, CONTRACTS, [day], fixed({"SiZ6": 9, "SiH7": 1}))
    again = roll.ensure_decisions(SI, repo, iss, CONTRACTS, [day], fixed({"SiZ6": 1, "SiH7": 9}))
    assert first == again == {day: "SiZ6"}
    assert repo.save_active_contract("2026-12-15", "SiH7", "volume", None, NOW.isoformat()) is False
    saved = repo.active_contract("2026-12-15")
    assert saved is not None and saved["ticker"] == "SiZ6" and saved["reason"] == "carry"


def test_roll_single_source(repo: MarketDataRepository) -> None:
    """SPEC-0014 A-5, G-1: догрузка, FINAL, стрим и читатель — из решения, не по SPEC-0013.

    Сверка берёт тикер дня из решения — ``test_reconciler_records_active_contract``.
    """
    day = date(2026, 12, 15)  # по SPEC-0013 — SiZ6
    repo.save_active_contract(day.isoformat(), "SiH7", "volume", None, NOW.isoformat())
    stamp = datetime(2026, 12, 15, tzinfo=UTC)
    assert archive_key_candidates(SI, stamp, FUTURES, repo) == ["SiH7_SPBFUT"]
    assert archive_key_candidates(SI, stamp, FUTURES) == ["SiZ6_SPBFUT"]
    assert current_figi(SI, {}, FUTURES, day, repo) == "FH7"
    assert contract_of(SI, repo, FUTURES, day) == H7  # FINAL: secid дня
    assert get_contract(repo, day) == "SiH7"


def _sdk_future(meta: FuturesMeta) -> SimpleNamespace:
    return SimpleNamespace(
        ticker=meta.ticker,
        figi=meta.figi,
        uid=f"uid-{meta.ticker}",
        class_code=meta.class_code,
        expiration_date=meta.expiration.isoformat(),
        last_trade_date=meta.active_until.isoformat(),
    )


async def test_roll_startup_local_only(
    repo: MarketDataRepository, iss: IssTapeRepository, monkeypatch: pytest.MonkeyPatch
) -> None:
    """SPEC-0014 A-6: запуск без решения на сегодня — решение по локальной ленте, без сети."""
    today = date(2026, 12, 15)
    monkeypatch.setattr(main_mod, "datetime", SimpleNamespace(now=lambda _tz=None: NOW))

    async def no_network(*_a: Any, **_k: Any) -> None:
        raise AssertionError("сеть при запуске")

    monkeypatch.setattr(main_mod.final_mod, "_totals", no_network)
    repo.save_active_contract("2026-12-14", "SiZ6", "carry", None, NOW.isoformat())
    tape(iss, "2026-12-14", [("SiZ6", 3), ("SiH7", 8)])
    tape(iss, "2026-12-11", [("SiZ6", 4), ("SiH7", 6)])
    futures = [_sdk_future(m) for m in (Z6, H7, M7)]
    client = SimpleNamespace(instruments=None)
    figi, ticker, uid = await main_mod._resolve_stream_figi(client, SI, futures, repo, iss)
    assert (figi, ticker, uid) == ("FH7", "SiH7", "uid-SiH7")
    saved = repo.active_contract(today.isoformat())
    assert saved is not None and saved["reason"] == "volume"
    assert str(saved["basis"]).endswith("лента")


def test_roll_rule_migration(
    repo: MarketDataRepository, iss: IssTapeRepository, caplog: pytest.LogCaptureFixture
) -> None:
    """SPEC-0014 A-7, G-4: день со сменой контракта — roll_rule, без смены — без отметки."""
    for day in ("2026-09-15", "2026-09-16", "2026-09-17", "2026-09-18"):
        stamp = f"{day}T10:00:00.000000+00:00"
        repo.insert_ticks(
            [
                {
                    "ticker": "Si_CONTINUOUS",
                    "timestamp": stamp,
                    "price": 1.0,
                    "volume": 1,
                    "direction": "BUY",
                    "trade_id": "t",
                }
            ]
        )
    repo.set_meta("contract:2026-09-16", "SiU6")  # прежняя запись сверки
    # 18.09: сверка видела SiZ6 (как решение), но день перекачан из архива по
    # SPEC-0013 — ближайший тоже SiZ6, расхождения нет; 17.09: сверка писала SiZ6,
    # а архив брал SiU6 (как BR 02.11) — день должен быть отмечен.
    repo.set_meta("contract:2026-09-17", "SiZ6")
    totals = {"SiU6": (1, 100.0), "SiZ6": (1, 50.0)}
    for day in ("2026-09-14", "2026-09-15", "2026-09-16"):
        iss.save_totals("forts", day, totals)
    caplog.set_level(logging.WARNING)
    decisions = roll.update_instrument(
        SI, repo, iss, FUTURES, date(2026, 9, 19), roll.LocalVolumes(iss)
    )
    # 17.09 — последний день торгов SiU6: по новому правилу уже SiZ6.
    assert decisions[date(2026, 9, 17)] == "SiZ6"
    assert decisions[date(2026, 9, 16)] == "SiU6"
    assert set(repo.days_to_refresh()) == {"2026-09-17"}
    assert repo.days_to_refresh()["2026-09-17"] == REFRESH_ROLL_RULE
    assert "ROLL_RULE Si_CONTINUOUS 2026-09-17: SiU6/SiZ6 -> SiZ6" in caplog.text
    # Перевод — один раз.
    repo.clear_refresh_mark("2026-09-17")
    roll.update_instrument(SI, repo, iss, FUTURES, date(2026, 9, 20), roll.LocalVolumes(iss))
    assert repo.days_to_refresh() == {}
