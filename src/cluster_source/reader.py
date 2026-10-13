"""API чтения для торгового робота: построение footprint-матрицы (кластеров).

:func:`get_cluster_data` объединяет горячий (SQLite) и холодный (Parquet)
источники тиков, округляет цены до шага цены инструмента и строит матрицу
цена x объём с разбивкой по покупателям/продавцам (Delta), как в footprint-
чартах TigerTrade.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime

import numpy as np
import pandas as pd

from cluster_source.config import AppConfig
from cluster_source.database import MarketDataRepository

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
