"""SPEC-0002: в хранилище попадают только биржевые сделки."""

from __future__ import annotations

import gzip
import logging
from datetime import UTC, datetime, timedelta
from enum import IntEnum
from types import SimpleNamespace
from typing import Any

import pandas as pd
import pytest

from cluster_source.autoload import (
    DEALER_CLEANUP_META_KEY,
    REFRESH_DEALER_CLEANUP,
    BackfillResult,
    _backfill_current_session,
    _backfill_from_archive,
    _exchange_only,
    _parse_history_csv,
    _RateGate,
    _settle_refresh_mark,
    _trades_to_rows,
    refresh_days,
    schedule_dealer_cleanup,
)
from cluster_source.collector import DataCollector
from cluster_source.config import (
    AppConfig,
    Instrument,
    InstrumentType,
    TradeSource,
    is_exchange_trade,
    parse_trade_source,
)
from cluster_source.database import MarketDataRepository

UTC = UTC
SBER = Instrument(id="SBER", type=InstrumentType.SHARE, tick_size=0.01, lot_size=1)
FMT = "%Y-%m-%d"


class SdkTradeSource(IntEnum):
    """Повторяет ``TradeSourceType`` SDK (IntEnum)."""

    TRADE_SOURCE_UNSPECIFIED = 0
    TRADE_SOURCE_EXCHANGE = 1
    TRADE_SOURCE_DEALER = 2
    TRADE_SOURCE_ALL = 3


def _today() -> datetime:
    return datetime.now(UTC).replace(hour=0, minute=0, second=0, microsecond=0)


def _row(ts: datetime, tid: str) -> dict[str, object]:
    return {
        "ticker": "SBER",
        "timestamp": ts,
        "price": 100.0,
        "volume": 1,
        "direction": "BUY",
        "trade_id": tid,
    }


# --------------------------------------------------------------------------- #
# Разбор источника сделки
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize(
    ("raw", "expected", "stored"),
    [
        (SdkTradeSource.TRADE_SOURCE_EXCHANGE, TradeSource.EXCHANGE, True),
        (SdkTradeSource.TRADE_SOURCE_DEALER, TradeSource.DEALER, False),
        (SdkTradeSource.TRADE_SOURCE_UNSPECIFIED, TradeSource.UNSPECIFIED, True),
        (SdkTradeSource.TRADE_SOURCE_ALL, TradeSource.OTHER, False),
        ("TRADE_SOURCE_DEALER", TradeSource.DEALER, False),
        ("EXCHANGE", TradeSource.EXCHANGE, True),
        ("dealer", TradeSource.DEALER, False),
        (2, TradeSource.DEALER, False),
        (None, TradeSource.UNSPECIFIED, True),
        ("", TradeSource.UNSPECIFIED, True),
    ],
)
def test_parse_trade_source(raw: object, expected: TradeSource, stored: bool) -> None:
    """SPEC-0002 G-1, G-3: явный небиржевой источник отбрасывается, неуказанный — нет."""
    assert parse_trade_source(raw) == expected
    assert is_exchange_trade(raw) is stored


# --------------------------------------------------------------------------- #
# A-1: подписка стрима
# --------------------------------------------------------------------------- #
def test_stream_subscription_requests_exchange_trades_only() -> None:
    """SPEC-0002 A-1 (G-2): подписка явно запрашивает TRADE_SOURCE_EXCHANGE."""
    schemas = pytest.importorskip("t_tech.invest.grpc.schemas")
    from cluster_source.main import build_trades_subscription

    request = build_trades_subscription("BBG004730N88")
    sub = request.subscribe_trades_request
    assert sub.trade_source == schemas.TradeSourceType.TRADE_SOURCE_EXCHANGE
    assert sub.instruments[0].instrument_id == "BBG004730N88"


# --------------------------------------------------------------------------- #
# A-2: GetLastTrades
# --------------------------------------------------------------------------- #
class _Resp:
    def __init__(self, status: int, json_body: Any = None, body: bytes = b"") -> None:
        self.status = status
        self._json = json_body
        self._body = body
        self.headers: dict[str, str] = {}

    async def __aenter__(self) -> _Resp:
        return self

    async def __aexit__(self, *_: object) -> None:
        return None

    def raise_for_status(self) -> None:
        return None

    async def json(self) -> Any:
        return self._json

    async def read(self) -> bytes:
        return self._body


class _Session:
    def __init__(self, resp: _Resp) -> None:
        self.resp = resp
        self.posted: list[Any] = []

    def post(self, _url: str, json: Any = None, **_: Any) -> _Resp:
        self.posted.append(json)
        return self.resp

    def get(self, _url: str, **_: Any) -> _Resp:
        return self.resp


async def test_last_trades_rest_requests_exchange_and_drops_dealer(
    share_config: AppConfig, share_repo: MarketDataRepository
) -> None:
    """SPEC-0002 A-2 (G-2): REST-запрос с TRADE_SOURCE_EXCHANGE; дилерские строки отброшены."""
    now = datetime.now(UTC)
    trades = [
        {"time": now.isoformat(), "price": 1.0, "quantity": 1, "direction": "1",
         "trade_source": "TRADE_SOURCE_EXCHANGE"},
        {"time": now.isoformat(), "price": 2.0, "quantity": 1, "direction": "1",
         "trade_source": "TRADE_SOURCE_DEALER"},
    ]  # fmt: skip
    session = _Session(_Resp(200, {"trades": trades}))
    result = await _backfill_current_session(
        session,  # type: ignore[arg-type]
        share_config,
        SBER,
        share_repo,
        _today(),
        "FIGI",
        market_data=None,
    )
    assert result == BackfillResult.WRITTEN
    assert session.posted[0]["trade_source"] == "TRADE_SOURCE_EXCHANGE"
    assert share_repo.count_hot() == 1


async def test_last_trades_sdk_drops_dealer(
    share_config: AppConfig, share_repo: MarketDataRepository
) -> None:
    """SPEC-0002 A-2: обёртка SDK не принимает trade_source — фильтр по полю сделки."""
    now = datetime.now(UTC)

    def trade(source: SdkTradeSource) -> Any:
        return SimpleNamespace(time=now, price=1.0, quantity=1, direction=1, trade_source=source)

    class MarketData:
        async def get_last_trades(self, instrument_id: str) -> Any:
            return SimpleNamespace(
                trades=[
                    trade(SdkTradeSource.TRADE_SOURCE_EXCHANGE),
                    trade(SdkTradeSource.TRADE_SOURCE_DEALER),
                    trade(SdkTradeSource.TRADE_SOURCE_DEALER),
                ]
            )

    session = _Session(_Resp(500))
    await _backfill_current_session(
        session,  # type: ignore[arg-type]
        share_config,
        SBER,
        share_repo,
        _today(),
        "FIGI",
        market_data=MarketData(),
    )
    assert share_repo.count_hot() == 1


def test_trades_to_rows_keeps_unspecified_source() -> None:
    """SPEC-0002 G-3: сделка без источника сохраняется (фильтр стоит в запросе)."""
    rows = _trades_to_rows([{"time": "2026-09-05T10:00:00Z", "price": 1.0}], SBER)
    assert len(rows) == 1


# --------------------------------------------------------------------------- #
# A-3: архив history-trades
# --------------------------------------------------------------------------- #
ARCHIVE_MIXED = """TRADE_TS,TICKER_CC,DIRECTION,PRICE,QUANTITY,TRADE_SOURCE,INSTRUMENT_UID
{d}T07:00:00.100000Z,SBER_TQBR,BUY,279.6,800,EXCHANGE,u
{d}T23:00:05.241966Z,SBER_TQBR,SELL,279.78,1,DEALER,u
{d}T23:00:05.573506Z,SBER_TQBR,SELL,279.52,6,DEALER,u
"""

ARCHIVE_DEALER_ONLY = """TRADE_TS,TICKER_CC,DIRECTION,PRICE,QUANTITY,TRADE_SOURCE,INSTRUMENT_UID
{d}T23:00:05.241966Z,SBER_TQBR,SELL,279.78,1,DEALER,u
"""


def test_archive_drops_dealer_rows() -> None:
    """SPEC-0002 A-3 (G-2): строки архива с TRADE_SOURCE=DEALER отбрасываются при разборе."""
    df = _parse_history_csv(ARCHIVE_MIXED.format(d="2026-09-04").encode())
    kept, dropped = _exchange_only(df)
    assert dropped == 2
    assert list(kept["price"]) == [279.6]


async def test_archive_backfill_writes_exchange_only(
    share_config: AppConfig, share_repo: MarketDataRepository
) -> None:
    """SPEC-0002 A-3: в хранилище из архива попадают только биржевые сделки."""
    day = _today() - timedelta(days=3)
    body = gzip.compress(ARCHIVE_MIXED.format(d=day.strftime(FMT)).encode())
    result = await _backfill_from_archive(
        _Session(_Resp(200, body=body)),  # type: ignore[arg-type]
        share_config,
        SBER,
        share_repo,
        day,
        ["SBER_TQBR"],
        _RateGate(0),
    )
    assert result == BackfillResult.WRITTEN
    assert share_repo.count_hot() == 1


async def test_archive_with_dealer_only_reports_no_exchange_trades(
    share_config: AppConfig, share_repo: MarketDataRepository
) -> None:
    """SPEC-0002 A-3: день только с дилерскими сделками — NO_EXCHANGE_TRADES, ничего не пишется."""
    day = _today() - timedelta(days=3)
    body = gzip.compress(ARCHIVE_DEALER_ONLY.format(d=day.strftime(FMT)).encode())
    result = await _backfill_from_archive(
        _Session(_Resp(200, body=body)),  # type: ignore[arg-type]
        share_config,
        SBER,
        share_repo,
        day,
        ["SBER_TQBR"],
        _RateGate(0),
    )
    assert result == BackfillResult.NO_EXCHANGE_TRADES
    assert share_repo.count_hot() == 0


# --------------------------------------------------------------------------- #
# A-4: стрим
# --------------------------------------------------------------------------- #
class _ListStream:
    def __init__(self, items: list[Any]) -> None:
        self._items = list(items)

    def __aiter__(self) -> _ListStream:
        return self

    async def __anext__(self) -> Any:
        if not self._items:
            raise StopAsyncIteration
        return self._items.pop(0)


async def test_stream_dealer_trade_dropped_and_counted(
    share_config: AppConfig, share_repo: MarketDataRepository, caplog: pytest.LogCaptureFixture
) -> None:
    """SPEC-0002 A-4 (G-3): дилерская сделка из стрима не попадает в очередь и учитывается."""
    now = datetime.now(UTC)

    def item(source: Any) -> Any:
        trade = SimpleNamespace(time=now, price=1.0, quantity=1, direction=1, trade_source=source)
        return SimpleNamespace(trade=trade)

    collector = DataCollector(share_config, SBER, share_repo)
    collector._running = True
    with caplog.at_level(logging.WARNING):
        await collector.subscribe(
            _ListStream(
                [
                    item(SdkTradeSource.TRADE_SOURCE_EXCHANGE),
                    item(SdkTradeSource.TRADE_SOURCE_DEALER),
                    item(SdkTradeSource.TRADE_SOURCE_UNSPECIFIED),
                ]
            )
        )
    assert collector._queue.qsize() == 2
    assert collector.dealer_trades_dropped == 1
    assert "DEALER_TRADE_DROPPED" in caplog.text


# --------------------------------------------------------------------------- #
# A-5: чистка ранее сохранённых данных
# --------------------------------------------------------------------------- #
def test_schedule_cleanup_marks_all_past_days_once(
    share_config: AppConfig, share_repo: MarketDataRepository
) -> None:
    """SPEC-0002 A-5 (G-4): отмечены все прошедшие дни обоих хранилищ, один раз."""
    today = _today()
    recent = today - timedelta(days=2)
    old = today - timedelta(days=share_config.history_check_days + 30)
    share_repo.insert_ticks(
        [_row(today + timedelta(hours=1), "t"), _row(recent + timedelta(hours=10), "r")]
    )
    share_repo.write_parquet(
        old.strftime(FMT), pd.DataFrame([_row(old + timedelta(hours=10), "o")])
    )
    share_repo.mark_days_for_refresh([recent.strftime(FMT)], "stream_gap")

    assert schedule_dealer_cleanup(share_repo) == 2
    marks = share_repo.days_to_refresh()
    assert marks == {
        recent.strftime(FMT): REFRESH_DEALER_CLEANUP,  # причина перезаписана
        old.strftime(FMT): REFRESH_DEALER_CLEANUP,
    }
    assert share_repo.get_meta(DEALER_CLEANUP_META_KEY) is not None

    share_repo.clear_refresh_mark(old.strftime(FMT))
    assert schedule_dealer_cleanup(share_repo) == 0  # повторно не отмечает
    assert old.strftime(FMT) not in share_repo.days_to_refresh()


def test_refresh_days_separates_cleanup_marks(
    share_config: AppConfig, share_repo: MarketDataRepository
) -> None:
    """SPEC-0002 G-4: отметки чистки не ограничены окном и не попадают в обычный проход."""
    today = _today()
    old = (today - timedelta(days=share_config.history_check_days + 30)).strftime(FMT)
    older = (today - timedelta(days=share_config.history_check_days + 31)).strftime(FMT)
    recent = (today - timedelta(days=2)).strftime(FMT)
    share_repo.mark_days_for_refresh([old, older], REFRESH_DEALER_CLEANUP)
    share_repo.mark_days_for_refresh([recent], "stream_gap")

    normal = [d.strftime(FMT) for d in refresh_days(share_config, share_repo)]
    cleanup = [d.strftime(FMT) for d in refresh_days(share_config, share_repo, cleanup=True)]
    assert normal == [recent]
    assert cleanup == [old, older]  # от новых к старым
    assert old in share_repo.days_to_refresh()  # окно их не снимает


def test_settle_no_exchange_trades_deletes_day(share_repo: MarketDataRepository) -> None:
    """SPEC-0002 G-1: архив подтвердил отсутствие биржевых сделок — день удаляется целиком."""
    day = _today() - timedelta(days=12)
    day_str = day.strftime(FMT)
    rows = [_row(day + timedelta(hours=23), "dealer")]
    share_repo.insert_ticks(rows)
    share_repo.write_parquet(day_str, pd.DataFrame(rows))
    share_repo.mark_days_for_refresh([day_str], "stream_gap")

    _settle_refresh_mark(share_repo, day, BackfillResult.NO_EXCHANGE_TRADES)

    assert day_str not in share_repo.available_days()
    assert day_str not in share_repo.days_to_refresh()


def test_settle_cleanup_404_on_published_day_deletes(share_repo: MarketDataRepository) -> None:
    """SPEC-0002 G-4: 404 по отметке чистки за давно опубликованный день — биржевых сделок нет."""
    old = _today() - timedelta(days=30)
    yesterday = _today() - timedelta(days=1)
    for day in (old, yesterday):
        share_repo.insert_ticks([_row(day + timedelta(hours=23), "dealer")])
        share_repo.mark_days_for_refresh([day.strftime(FMT)], REFRESH_DEALER_CLEANUP)

    _settle_refresh_mark(share_repo, old, BackfillResult.NO_DATA)
    _settle_refresh_mark(share_repo, yesterday, BackfillResult.NO_DATA)

    assert old.strftime(FMT) not in share_repo.available_days()
    assert old.strftime(FMT) not in share_repo.days_to_refresh()
    # Вчерашний архив мог ещё не выйти: данные и отметка остаются.
    assert yesterday.strftime(FMT) in share_repo.available_days()
    assert yesterday.strftime(FMT) in share_repo.days_to_refresh()


async def test_run_dealer_cleanup_until_no_marks_left(
    share_config: AppConfig, share_repo: MarketDataRepository, monkeypatch: pytest.MonkeyPatch
) -> None:
    """SPEC-0002 G-4: чистка отмечает дни и повторяет проходы cleanup=True до конца отметок."""
    import asyncio

    from cluster_source import main

    day = _today() - timedelta(days=20)
    share_repo.insert_ticks([_row(day + timedelta(hours=10), "x")])
    calls: list[dict[str, Any]] = []

    async def fake_backfill(
        config: AppConfig, repos: dict[str, MarketDataRepository], **kwargs: Any
    ) -> None:
        calls.append(kwargs)
        for repo in repos.values():
            for day_str in repo.days_to_refresh():
                repo.clear_refresh_mark(day_str)

    monkeypatch.setattr(main, "_run_backfill", fake_backfill)
    monkeypatch.setattr(main, "CLEANUP_RETRY_SEC", 0.0)
    lock = asyncio.Lock()
    gate = _RateGate(0)

    await main.run_dealer_cleanup(share_config, {"SBER": share_repo}, gate=gate, lock=lock)

    assert len(calls) == 1
    assert calls[0] == {"gate": gate, "lock": lock, "cleanup": True}
    assert share_repo.get_meta(DEALER_CLEANUP_META_KEY) is not None
