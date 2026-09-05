"""Точка входа в приложение.

Связывает через явную Dependency Injection конфигурацию, репозитории,
автозагрузчик, ночной архиватор и реальновременные коллекторы. Обрабатывает
SIGINT/SIGTERM для корректного завершения: стриминг останавливается, консьюмер
сбрасывает все накопленные тики в SQLite, дескрипторы БД закрываются, и только
потом процесс завершается.
"""

from __future__ import annotations

import asyncio
import logging
import signal
import sys
from pathlib import Path
from typing import Any

from cluster_source import archiver as archiver_mod
from cluster_source import autoload
from cluster_source.collector import DataCollector, StreamFactory, StreamHandle
from cluster_source.config import AppConfig, Instrument, InstrumentType
from cluster_source.database import MarketDataRepository

logger = logging.getLogger("cluster_source")

CONFIG_PATH = Path("config.yaml")

# Необязательный импорт SDK: gRPC-стриминг работает только когда установлен
# T-Invest Python SDK (это жёсткая зависимость, но такой импорт держит модуль
# импортируемым для юнит-тестов, проверяющих логику репозитория/ридера).
# Текущий SDK — `t-tech-investments` (модуль `t_tech`); легаси-пакет
# `tinkoff-investments` предоставляет тот же API стриминга для конвертации
# DTO ниже.
try:  # pragma: no cover - depends on environment
    from t_tech.invest import AsyncClient  # type: ignore[import-not-found,unused-ignore]
    from t_tech.invest.grpc.common import (  # type: ignore[import-not-found,unused-ignore]
        InstrumentStatus as _SdkInstrumentStatus,
    )
    from t_tech.invest.grpc.common import (
        InstrumentType as _SdkInstrumentType,
    )
    from t_tech.invest.grpc.schemas import (  # type: ignore[import-not-found,unused-ignore]
        MarketDataRequest,
        SubscribeTradesRequest,
        SubscriptionAction,
        TradeInstrument,
    )

    _has_sdk = True
except ImportError:  # pragma: no cover
    AsyncClient = None  # type: ignore[misc,assignment]
    _SdkInstrumentStatus = None  # type: ignore[misc,assignment]
    _SdkInstrumentType = None  # type: ignore[misc,assignment]
    MarketDataRequest = None  # type: ignore[misc,assignment]
    SubscriptionAction = None  # type: ignore[misc,assignment]
    SubscribeTradesRequest = None  # type: ignore[misc,assignment]
    TradeInstrument = None  # type: ignore[misc,assignment]
    _has_sdk = False


def setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        handlers=[
            logging.FileHandler("cluster_source.log", encoding="utf-8"),
            logging.StreamHandler(sys.stdout),
        ],
        force=True,
    )


async def _resolve_stream_figi(client: Any, instrument: Instrument) -> str:
    """Разрешает FIGI, на который должен подписаться коллектор.

    Для ``continuous_futures`` (например, ``Si``) через сервис инструментов
    находится ближайший к истечению активный контракт, чтобы мы всегда
    отслеживали текущий ликвидный контракт, сохраняя под непрерывным ключом.
    """
    instruments_service = client.instruments
    if instrument.is_continuous or instrument.type == InstrumentType.FUTURES_CONTRACT:
        futures = await instruments_service.futures(
            instrument_status=_SdkInstrumentStatus.INSTRUMENT_STATUS_BASE
        )
        if instrument.is_continuous:
            eligible = [f for f in futures.instruments if f.ticker.startswith(instrument.id)]
            if not eligible:
                raise RuntimeError(
                    f"Не найден активный фьючерсный контракт "
                    f"для непрерывного инструмента {instrument.id}"
                )
            active = min(eligible, key=lambda f: f.expiration_date)
            logger.info(
                "Непрерывный %s -> активный контракт %s (истекает %s)",
                instrument.id,
                active.ticker,
                active.expiration_date,
            )
            return str(active.figi)
        matches = [f for f in futures.instruments if f.ticker == instrument.id]
        if not matches:
            raise RuntimeError(f"Фьючерсный контракт не найден: {instrument.id}")
        return str(matches[0].figi)

    share = await instruments_service.find_instrument(
        query=instrument.id,
        instrument_kind=_SdkInstrumentType.INSTRUMENT_TYPE_SHARE,
    )
    instruments = getattr(share, "instruments", None) or []
    for item in instruments:
        if item.ticker == instrument.id:
            return str(item.figi)
    raise RuntimeError(f"Тикер не найден: {instrument.id}")


def make_stream_factory(config: AppConfig) -> StreamFactory:
    """Привязывает конфиг к вызываемому stream factory, инжектируемому в коллекторы."""

    async def factory(instrument: Instrument) -> StreamHandle:
        return await create_stream(config, instrument)

    return factory


async def create_stream(config: AppConfig, instrument: Instrument) -> StreamHandle:
    """Открывает gRPC-стрим сделок для ``instrument``.

    Для ``continuous_futures`` сначала резолвится ближайший к истечению
    (активный) контракт, но все тики сохраняются под непрерывным ключом.
    """
    if not _has_sdk:
        raise RuntimeError("t-tech-investments не установлен; стриминг рыночных данных недоступен")

    client = AsyncClient(config.token)
    services = await client.__aenter__()
    figi = await _resolve_stream_figi(services, instrument)
    stream = services.create_market_data_stream()
    stream.subscribe(
        MarketDataRequest(
            subscribe_trades_request=SubscribeTradesRequest(
                subscription_action=SubscriptionAction.SUBSCRIPTION_ACTION_SUBSCRIBE,
                instruments=[TradeInstrument(instrument_id=figi)],
            )
        )
    )

    async def teardown() -> None:
        try:
            stream.stop()
        finally:
            await client.__aexit__(None, None, None)  # type: ignore[no-untyped-call]

    return StreamHandle(stream=stream, teardown=teardown)


async def amain(config: AppConfig) -> int:
    repos: dict[str, MarketDataRepository] = {}
    for instrument in config.instruments:
        repos[instrument.storage_name] = MarketDataRepository(config.storage_root, instrument)

    logger.info(
        "Проверка автозагрузки/догрузки истории (последние %d дней)",
        config.history_check_days,
    )
    sdk_client: Any = None
    try:
        if _has_sdk:
            sdk_client = AsyncClient(config.token)
            services = await sdk_client.__aenter__()
            await autoload.run_backfill(
                config,
                repos,
                market_data=services.market_data,
                instruments=services.instruments,
                kind_share=_SdkInstrumentType.INSTRUMENT_TYPE_SHARE,
                kind_futures=_SdkInstrumentType.INSTRUMENT_TYPE_FUTURES,
                futures_status=_SdkInstrumentStatus.INSTRUMENT_STATUS_BASE,
            )
        else:
            await autoload.run_backfill(config, repos)
    except Exception as exc:  # noqa: BLE001
        logger.error("Ошибка автозагрузки: %s", exc)
    finally:
        if sdk_client is not None:
            await sdk_client.__aexit__(None, None, None)

    logger.info("Сверка архиватора")
    try:
        await archiver_mod.run_archiver(config, repos)
    except Exception as exc:  # noqa: BLE001
        logger.error("Ошибка сверки архиватора: %s", exc)

    collectors: list[DataCollector] = []
    stream_factory = make_stream_factory(config)
    for instrument in config.instruments:
        repo = repos[instrument.storage_name]
        logger.info("Подготовка коллектора для %s (%s)", instrument.id, instrument.type.value)
        collectors.append(
            DataCollector(
                config=config,
                instrument=instrument,
                repo=repo,
                stream_factory=stream_factory,
            )
        )

    stop_event = asyncio.Event()

    def _on_signal() -> None:
        logger.info("Получен сигнал завершения — останавливаем коллекторы")
        stop_event.set()

    if sys.platform != "win32":
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, _on_signal)
            except (NotImplementedError, RuntimeError):  # pragma: no cover
                pass

    collector_tasks = [asyncio.create_task(c.run()) for c in collectors]

    try:
        await stop_event.wait()
    finally:
        logger.info("Запрошено корректное завершение; сбрасываем накопленные тики в SQLite")
        for task in collector_tasks:
            task.cancel()
        await asyncio.gather(*collector_tasks, return_exceptions=True)
        for collector in collectors:
            await collector.shutdown()
        logger.info("Все коллекторы опустошены; соединения с БД закрыты")
    return 0


def main() -> int:
    setup_logging()
    config_path = Path("config.local.yaml") if Path("config.local.yaml").exists() else CONFIG_PATH
    config = AppConfig.from_yaml(config_path)
    if not config.token or config.token == "your-token-here":
        logger.warning(
            "Не задан API-токен — стриминг не заработает. "
            "Поместите токен в config.local.yaml (git-ignored) или "
            "оставьте заглушку в config.yaml.",
        )
    try:
        return asyncio.run(amain(config))
    except KeyboardInterrupt:  # pragma: no cover
        logger.info("Прервано пользователем")
        return 0


if __name__ == "__main__":
    sys.exit(main())
