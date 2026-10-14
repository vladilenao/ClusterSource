"""API чтения для торгового робота: построение footprint-матрицы (кластеров).

:func:`get_cluster_data` объединяет горячий (SQLite) и холодный (Parquet)
источники тиков, округляет цены до шага цены инструмента и строит матрицу
цена x объём с разбивкой по покупателям/продавцам (Delta), как в footprint-
чартах TigerTrade.
"""

from __future__ import annotations

import logging
from datetime import UTC, date, datetime

import numpy as np
import pandas as pd

from cluster_source.config import AppConfig
from cluster_source.database import IssTapeRepository, MarketDataRepository, SignalRepository

logger = logging.getLogger(__name__)

RESULT_COLUMNS = ["timestamp", "price", "buy_volume", "sell_volume", "delta"]


def _round_price(series: pd.Series, tick_size: float) -> pd.Series:
    return (series / tick_size).round().astype(float) * tick_size


def get_cluster_data(
    config: AppConfig,
    repo: MarketDataRepository,
    ticker: str,
    start_time: datetime,
    end_time: datetime,
    timeframe: str = "5min",
) -> pd.DataFrame:
    """Возвращает footprint-данные для ``ticker`` за [``start_time``, ``end_time``).

    Автоматически объединяет SQLite (свежие) и Parquet (историю): за день,
    у которого есть Parquet-файл, берётся Parquet, иначе SQLite. Цены
    округляются до ``tick_size`` инструмента перед группировкой. Без сделок в
    интервале возвращается пустой DataFrame с колонками результата.
    """
    instrument = next(
        (i for i in config.instruments if i.storage_name == ticker),
        None,
    )
    if instrument is None:
        instrument = repo.instrument
    tick_size = float(instrument.tick_size)

    if start_time.tzinfo is None:
        start_time = start_time.replace(tzinfo=UTC)
    if end_time.tzinfo is None:
        end_time = end_time.replace(tzinfo=UTC)

    frames: list[pd.DataFrame] = []
    try:
        frames.append(repo.read_parquet(start_time, end_time))
    except Exception as exc:  # pragma: no cover - defensive
        logger.warning("Ошибка чтения Parquet для %s: %s", ticker, exc)

    # После архивации день живёт и в горячем SQLite (окно 7 дней), и в Parquet.
    # Источник выбирается по дню целиком: Parquet — архивная копия SQLite
    # либо перекачанный официальный архив, поэтому у него приоритет. Так
    # сделки не учитываются дважды, а одинаковые сделки не схлопываются.
    hot = repo.read_ticks(start_time, end_time, ticker=ticker)
    if not hot.empty:
        cold_days = repo.parquet_days_available()
        hot = hot[~hot["timestamp"].dt.strftime("%Y-%m-%d").isin(cold_days)]
    frames.append(hot)

    non_empty = [f for f in frames if not f.empty]
    if not non_empty:
        return pd.DataFrame(columns=RESULT_COLUMNS)
    df = pd.concat(non_empty, ignore_index=True)
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)

    df["price"] = _round_price(df["price"].astype(float), tick_size)
    df["volume"] = df["volume"].astype(int)
    df["direction"] = df["direction"].astype(str).str.upper()

    df["buy_volume"] = np.where(df["direction"] == "BUY", df["volume"], 0)
    df["sell_volume"] = np.where(df["direction"] == "SELL", df["volume"], 0)

    result = (
        df.groupby([pd.Grouper(key="timestamp", freq=timeframe), "price"])
        .agg(
            buy_volume=("buy_volume", "sum"),
            sell_volume=("sell_volume", "sum"),
        )
        .reset_index()
    )
    result["delta"] = result["buy_volume"] - result["sell_volume"]
    result = result.sort_values(["timestamp", "price"]).reset_index(drop=True)
    return result[RESULT_COLUMNS]


def get_degraded_minutes(
    repo: MarketDataRepository, start_time: datetime, end_time: datetime
) -> pd.DataFrame:
    """Минуты со статусом ``DEGRADED`` в ``[start_time, end_time)`` (SPEC-0004).

    Колонки: ``minute`` (начало минуты, UTC) и ``reasons`` (причины через
    запятую). Минуты, которых нет в результате, — ``PROVISIONAL``. Сигнал,
    в окне которого есть такая минута, выдавать нельзя (SPEC-0001 G-1).
    """
    if start_time.tzinfo is None:
        start_time = start_time.replace(tzinfo=UTC)
    if end_time.tzinfo is None:
        end_time = end_time.replace(tzinfo=UTC)
    return repo.read_degraded_minutes(start_time, end_time)


def get_data_state(repo: MarketDataRepository) -> dict[str, object]:
    """Текущее состояние данных инструмента: ``state`` (``OK`` / ``DEGRADED``),
    ``reasons`` и ``since`` — момент последнего перехода (SPEC-0004 G-2).

    До первого перехода — ``{"state": "UNKNOWN", ...}``: процесс сбора ещё не
    сообщал о состоянии инструмента.
    """
    state = repo.quality_state()
    if state is None:
        return {"state": "UNKNOWN", "reasons": [], "since": None}
    return state


def get_minute_verdicts(
    repo: MarketDataRepository, start_time: datetime, end_time: datetime
) -> pd.DataFrame:
    """Вердикты сверки с лентой Мосбиржи в ``[start_time, end_time)`` (SPEC-0005).

    Колонки: ``minute`` (UTC), ``status`` (``VERIFIED`` / ``REPAIRED`` / ``GAP``),
    ``stream_n``, ``iss_n``, ``repaired`` (заменено секунд), ``detail``. Минуты
    без сделок вердикта не имеют: раньше :func:`get_verified_until` они сверены.
    """
    if start_time.tzinfo is None:
        start_time = start_time.replace(tzinfo=UTC)
    if end_time.tzinfo is None:
        end_time = end_time.replace(tzinfo=UTC)
    return repo.read_minute_verdicts(start_time, end_time)


def get_verified_until(repo: MarketDataRepository) -> datetime | None:
    """Момент, до которого данные инструмента сверены с лентой Мосбиржи (SPEC-0005)."""
    stored = repo.verified_until()
    return datetime.fromisoformat(stored) if stored else None


def get_day_verdicts(repo: MarketDataRepository) -> pd.DataFrame:
    """Вердикты торговых дней (SPEC-0006): ``FINAL`` / ``MISMATCH`` / ``UNVERIFIABLE``.

    Колонки: ``trade_date``, ``status``, ``ours_n``, ``ref_n``, ``ours_volume``,
    ``ref_volume``, ``reference`` (``iss_totals`` — акции, ``iss_tape`` —
    фьючерсы), ``detail``, ``checked_at``.
    """
    return repo.day_verdicts()


def get_final_days(repo: MarketDataRepository) -> list[date]:
    """Торговые дни со статусом ``FINAL`` — только по ним считаются пороги
    и базовые уровни (SPEC-0001 G-3, SPEC-0006 G-2)."""
    df = repo.day_verdicts()
    return [date.fromisoformat(d) for d in df.loc[df["status"] == "FINAL", "trade_date"]]


#: Окно норм SPEC-0008 по умолчанию, торговых дней.
BASELINE_DAYS = 20


def get_baseline_days(
    repo: MarketDataRepository, n: int = BASELINE_DAYS, allow_archive: bool = True
) -> list[tuple[date, str]]:
    """Дни для расчёта норм (RVOL, перцентили, профили): ``[(торговый день, статус)]``.

    Берутся последние ``n`` дней ``FINAL``; если их меньше ``n`` и
    ``allow_archive`` — окно добирается самыми свежими ``ARCHIVE_CONSISTENT``.
    ``MISMATCH`` / ``UNVERIFIABLE`` не берутся никогда (SPEC-0001 G-3, SPEC-0006).
    """
    df = repo.day_verdicts().sort_values("trade_date", ascending=False)
    final = [
        (date.fromisoformat(d), "FINAL") for d in df.loc[df["status"] == "FINAL", "trade_date"]
    ]
    chosen = final[:n]
    if allow_archive and len(chosen) < n:
        archive = df.loc[df["status"] == "ARCHIVE_CONSISTENT", "trade_date"]
        chosen += [(date.fromisoformat(d), "ARCHIVE_CONSISTENT") for d in archive][
            : n - len(chosen)
        ]
    return sorted(chosen)


def baseline_quality(days: list[tuple[date, str]]) -> str:
    """``pure`` — все дни окна ``FINAL``; ``mixed`` — есть архивные; ``empty`` — дней нет."""
    if not days:
        return "empty"
    return "pure" if all(status == "FINAL" for _, status in days) else "mixed"


# ---------------------------------------------------------------------- #
# Открытый интерес (SPEC-0007)
# ---------------------------------------------------------------------- #
OI_FLOW_COLUMNS = [
    "timestamp",
    "oi_close",
    "oi_change",
    "opened",
    "closed",
    "transferred",
    "outside",
    "trades",
]


def get_open_interest(
    repo: MarketDataRepository, start_time: datetime, end_time: datetime
) -> pd.DataFrame:
    """Ряд открытого интереса из стрима T-Invest (только фьючерсы): ``ts``, ``value``.

    Значение приходит при каждом изменении ОИ. ОИ Мосбиржи считает позиции
    обеих сторон. Минуты ``DEGRADED`` (SPEC-0004) — и для ОИ недостоверны.
    Хранится 7 дней; долгая история — :func:`get_oi_flow` по ленте ISS.
    """
    if start_time.tzinfo is None:
        start_time = start_time.replace(tzinfo=UTC)
    if end_time.tzinfo is None:
        end_time = end_time.replace(tzinfo=UTC)
    return repo.read_open_interest(start_time, end_time)


def get_contract(repo: MarketDataRepository, day: date) -> str | None:
    """Тикер активного контракта непрерывного фьючерса за день (записывает сверка)."""
    return repo.get_meta(f"contract:{day.isoformat()}")


def get_oi_flow(
    iss_repo: IssTapeRepository,
    secid: str,
    start_time: datetime,
    end_time: datetime,
    timeframe: str = "1min",
) -> pd.DataFrame:
    """Поток открытого интереса по ленте ISS (задержка 15 минут), по барам ``timeframe``.

    Для каждой сделки контракта (в порядке заключения) ΔОИ — разница
    ``OPENPOSITION`` с предыдущей сделкой; ОИ Мосбиржи считает обе стороны,
    поэтому: ``opened`` = max(ΔОИ, 0)/2, ``closed`` = max(−ΔОИ, 0)/2 (в пределах
    количества), ``transferred`` = количество − |ΔОИ|/2, ``outside`` — изменение
    ОИ сделками вне анонимной ленты (|ΔОИ| > 2 × количество). Колонки:
    ``timestamp`` (начало бара, UTC), ``oi_close``, ``oi_change``, ``opened``,
    ``closed``, ``transferred``, ``outside``, ``trades``.
    """
    if start_time.tzinfo is None:
        start_time = start_time.replace(tzinfo=UTC)
    if end_time.tzinfo is None:
        end_time = end_time.replace(tzinfo=UTC)
    # Запас назад — чтобы знать ОИ до первой сделки окна.
    rows = iss_repo.read_oi_trades(secid, start_time - pd.Timedelta(hours=3), end_time)
    if not rows:
        return pd.DataFrame(columns=OI_FLOW_COLUMNS)
    df = pd.DataFrame(rows, columns=["timestamp", "quantity", "oi"])
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, format="ISO8601")
    df["delta"] = df["oi"].diff()
    df = df[(df["timestamp"] >= start_time) & df["delta"].notna()].copy()
    if df.empty:
        return pd.DataFrame(columns=OI_FLOW_COLUMNS)
    half = (df["delta"].abs() / 2).clip(upper=df["quantity"])
    df["opened"] = np.where(df["delta"] > 0, half, 0.0)
    df["closed"] = np.where(df["delta"] < 0, half, 0.0)
    df["transferred"] = df["quantity"] - half
    df["outside"] = (df["delta"].abs() / 2 - df["quantity"]).clip(lower=0)
    bars = (
        df.groupby(pd.Grouper(key="timestamp", freq=timeframe))
        .agg(
            oi_close=("oi", "last"),
            oi_change=("delta", "sum"),
            opened=("opened", "sum"),
            closed=("closed", "sum"),
            transferred=("transferred", "sum"),
            outside=("outside", "sum"),
            trades=("quantity", "size"),
        )
        .reset_index()
    )
    bars = bars[bars["trades"] > 0].reset_index(drop=True)
    return bars[OI_FLOW_COLUMNS]


# ---------------------------------------------------------------------- #
# Сигналы (SPEC-0008)
# ---------------------------------------------------------------------- #
def _utc_iso(moment: datetime) -> str:
    moment = moment if moment.tzinfo else moment.replace(tzinfo=UTC)
    return moment.astimezone(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")


def get_signals(
    repo: SignalRepository, start: datetime, end: datetime, ticker: str | None = None
) -> pd.DataFrame:
    """Сигналы с концом окна в ``[start, end)``: статус ``PENDING`` / ``CONFIRMED`` /
    ``REVOKED`` / ``SUPPRESSED``, ``outcome`` / ``reason``, событие (``event_json``)."""
    return repo.read_signals(_utc_iso(start), _utc_iso(end), ticker)


def get_signal_stats(repo: SignalRepository, start: datetime, end: datetime) -> pd.DataFrame:
    """Режим наблюдения (SPEC-0001): по инструменту и типу — выдано, подтверждено,
    отозвано по причинам, отсеяно, доля отзывов и медианная задержка итога, мин."""
    df = repo.read_signals(_utc_iso(start), _utc_iso(end))
    columns = ["ticker", "event_type", "status", "reason", "count"]
    if df.empty:
        return pd.DataFrame(columns=[*columns, "revoked_share", "verify_delay_min"])
    df["reason"] = df["reason"].fillna("")
    counts = df.groupby(["ticker", "event_type", "status", "reason"]).size().reset_index()
    counts.columns = columns
    issued = df.loc[df["status"] != "SUPPRESSED"].groupby(["ticker", "event_type"]).size()
    revoked = df.loc[df["status"] == "REVOKED"].groupby(["ticker", "event_type"]).size()
    share = (revoked / issued).fillna(0.0).rename("revoked_share")
    resolved = df.loc[df["status"].isin(["CONFIRMED", "REVOKED"])]
    delay = (
        pd.to_datetime(resolved["resolved_at"], utc=True, format="ISO8601")
        - pd.to_datetime(resolved["window_to"], utc=True, format="ISO8601")
    ).dt.total_seconds() / 60.0
    delays = delay.groupby([resolved["ticker"], resolved["event_type"]]).median()
    out = counts.merge(share.reset_index(), on=["ticker", "event_type"], how="left")
    out = out.merge(
        delays.rename("verify_delay_min").reset_index(), on=["ticker", "event_type"], how="left"
    )
    return out
