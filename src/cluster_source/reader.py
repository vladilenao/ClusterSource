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


def _round_price(series: pd.Series, tick_size: float) -> pd.Series:
    return (series / tick_size).round().astype(float) * tick_size


def _sqlite_only(repo: MarketDataRepository) -> bool:
    """True, когда у репозитория вообще нет холодного хранилища."""
    return not repo._storage_dir.exists() or not any(repo._storage_dir.glob("*.parquet"))


def get_cluster_data(
    config: AppConfig,
    repo: MarketDataRepository,
    ticker: str,
    start_time: datetime,
    end_time: datetime,
    timeframe: str = "5min",
) -> pd.DataFrame:
    """Возвращает footprint-данные для ``ticker`` за [``start_time``, ``end_time``).

    Автоматически объединяет SQLite (свежие) и Parquet (историю). Цены
    округляются до ``tick_size`` инструмента перед группировкой.
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
    frames.append(repo.read_ticks(start_time, end_time, ticker=ticker))

    df = pd.concat([f for f in frames if f is not None and not f.empty], ignore_index=True)
    if df.empty:
        return pd.DataFrame(columns=["price", "buy_volume", "sell_volume", "delta"])

    # Один и тот же день живёт и в горячем SQLite (окно 7 дней), и, после
    # архивации, в холодном Parquet — идентичные строки. Удаляем точные дубли,
    # чтобы дни, догруженные из архива, не учитывались дважды, пока находятся
    # в горячем окне.
    dup_cols = ["ticker", "timestamp", "price", "volume", "direction", "trade_id"]
    df = df.drop_duplicates(subset=[c for c in dup_cols if c in df.columns], keep="last")

    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
    if df["timestamp"].dt.tz is None:
        df["timestamp"] = df["timestamp"].dt.tz_localize("UTC")

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
    return result
