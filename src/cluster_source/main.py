"""Точка входа в приложение.

Связывает через явную Dependency Injection конфигурацию, репозитории,
автозагрузчик, архиватор (при старте и ежесуточно по расписанию) и
реальновременные коллекторы. Обрабатывает
SIGINT/SIGTERM для корректного завершения: стриминг останавливается, консьюмер
сбрасывает все накопленные тики в SQLite, дескрипторы БД закрываются, и только
потом процесс завершается.
"""

from __future__ import annotations

import asyncio
import logging
import os
import shutil
import signal
import subprocess
import sys
from collections.abc import Awaitable, Callable, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from cluster_source import archiver as archiver_mod
from cluster_source import autoload
from cluster_source.collector import DataCollector, InstrumentNotFound
from cluster_source.config import AppConfig, Instrument, InstrumentType, nearest_contract
from cluster_source.database import IssTapeRepository, MarketDataRepository
from cluster_source.iss import IssCollector
from cluster_source.stream_hub import (
    MAX_SUBSCRIPTIONS_PER_STREAM,
    ConnectionFactory,
    HubConnection,
    StreamHub,
)

logger = logging.getLogger("cluster_source")

CONFIG_PATH = Path("config.yaml")

# gRPC по умолчанию разрешает имена встроенным резолвером c-ares. За VPN/прокси
# с собственным DNS (режим fake-ip, интерфейс 198.18.0.0/15) он отвечает
# «Could not contact DNS servers» и стримы не открываются; системный резолвер
# работает. Задаётся до импорта SDK (gRPC читает переменную при инициализации);
# значение из окружения не перезаписывается.
os.environ.setdefault("GRPC_DNS_RESOLVER", "native")

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
        InfoInstrument,
        MarketDataRequest,
        SubscribeInfoRequest,
        SubscribeTradesRequest,
        SubscriptionAction,
        TradeInstrument,
        TradeSourceType,
    )

    _has_sdk = True
except ImportError:  # pragma: no cover
    AsyncClient = None  # type: ignore[misc,assignment]
    _SdkInstrumentStatus = None  # type: ignore[misc,assignment]
    _SdkInstrumentType = None  # type: ignore[misc,assignment]
    InfoInstrument = None  # type: ignore[misc,assignment]
    MarketDataRequest = None  # type: ignore[misc,assignment]
    SubscribeInfoRequest = None  # type: ignore[misc,assignment]
    SubscriptionAction = None  # type: ignore[misc,assignment]
    SubscribeTradesRequest = None  # type: ignore[misc,assignment]
    TradeInstrument = None  # type: ignore[misc,assignment]
    TradeSourceType = None  # type: ignore[misc,assignment]
    _has_sdk = False

#: Пауза между проходами чистки от дилерских сделок, если остались необработанные дни.
CLEANUP_RETRY_SEC = 1800.0
#: Не больше стольких проходов чистки за запуск (~сутки); остаток — при следующем запуске.
CLEANUP_MAX_PASSES = 48


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


async def _resolve_stream_figi(
    client: Any, instrument: Instrument, futures: list[Any] | None = None
) -> str:
    """Разрешает FIGI, на который подписывается стрим для инструмента.

    Для ``continuous_futures`` (например, ``Si``) через сервис инструментов
    находится ближайший к истечению активный контракт, чтобы мы всегда
    отслеживали текущий ликвидный контракт, сохраняя под непрерывным ключом.
    ``futures`` — уже полученный справочник фьючерсов (один запрос на все).
    """
    instruments_service = client.instruments
    if instrument.is_continuous or instrument.type == InstrumentType.FUTURES_CONTRACT:
        if futures is None:
            response = await instruments_service.futures(
                instrument_status=_SdkInstrumentStatus.INSTRUMENT_STATUS_BASE
            )
            futures = list(response.instruments)
        if instrument.is_continuous:
            metas = autoload.futures_from_sdk(futures)
            active = nearest_contract(instrument, metas.values(), datetime.now(UTC).date())
            if active is None:
                raise InstrumentNotFound(
                    f"Не найден активный фьючерсный контракт "
                    f"для непрерывного инструмента {instrument.id}"
                )
            logger.info(
                "Непрерывный %s -> активный контракт %s (истекает %s)",
                instrument.id,
                active.ticker,
                active.expiration,
            )
            return active.figi
        matches = [f for f in futures if f.ticker == instrument.id]
        if not matches:
            raise InstrumentNotFound(f"Фьючерсный контракт не найден: {instrument.id}")
        return str(matches[0].figi)

    share = await instruments_service.find_instrument(
        query=instrument.id,
        instrument_kind=_SdkInstrumentType.INSTRUMENT_TYPE_SHARE,
    )
    instruments = getattr(share, "instruments", None) or []
    ticker_matches = [item for item in instruments if item.ticker == instrument.id]
    if not ticker_matches:
        raise InstrumentNotFound(f"Тикер не найден: {instrument.id}")
    # Один и тот же тикер может встречаться на нескольких площадках
    # (например, "T" -> AT&T и TQBR "Т-Технологии"); для спот-акций
    # подходит только бумага основной секции МосБиржи.
    for preferred in ("TQBR", "SPBXM"):
        for item in ticker_matches:
            if getattr(item, "class_code", "") == preferred:
                return str(item.figi)
    return str(ticker_matches[0].figi)


def build_info_subscription(figis: list[str]) -> Any:
    """Подписка того же соединения на торговые статусы (SPEC-0004: детектор тишины).

    Сервер сразу присылает текущий статус каждого инструмента; лимита на число
    таких подписок нет.
    """
    return MarketDataRequest(
        subscribe_info_request=SubscribeInfoRequest(
            subscription_action=SubscriptionAction.SUBSCRIPTION_ACTION_SUBSCRIBE,
            instruments=[InfoInstrument(instrument_id=figi) for figi in figis],
        )
    )


def build_trades_subscription(figis: list[str]) -> Any:
    """Запрос подписки одного стрима на сделки всех инструментов ``figis``.

    SPEC-0009: одна подписка в одном соединении (до 300 инструментов).
    SPEC-0002 G-2: только биржевые сделки. По умолчанию сервер отдаёт и
    дилерские (``TRADE_SOURCE_ALL``), поэтому источник задаётся явно.
    """
    return MarketDataRequest(
        subscribe_trades_request=SubscribeTradesRequest(
            subscription_action=SubscriptionAction.SUBSCRIPTION_ACTION_SUBSCRIBE,
            instruments=[TradeInstrument(instrument_id=figi) for figi in figis],
            trade_source=TradeSourceType.TRADE_SOURCE_EXCHANGE,
        )
    )


async def _resolve_figis(
    services: Any, instruments: list[Instrument]
) -> tuple[dict[str, str], list[str]]:
    """FIGI всех инструментов ``{storage_name: figi}`` и список ненайденных.

    Справочник фьючерсов запрашивается один раз на все инструменты.
    """
    futures: list[Any] | None = None
    if any(i.type != InstrumentType.SHARE for i in instruments):
        response = await services.instruments.futures(
            instrument_status=_SdkInstrumentStatus.INSTRUMENT_STATUS_BASE
        )
        futures = list(response.instruments)
    resolved: dict[str, str] = {}
    not_found: list[str] = []
    for instrument in instruments:
        try:
            resolved[instrument.storage_name] = await _resolve_stream_figi(
                services, instrument, futures
            )
        except InstrumentNotFound as exc:
            logger.warning("%s", exc)
            not_found.append(instrument.storage_name)
    return resolved, not_found


async def _log_stream_limits(services: Any) -> None:
    """Пишет в лог лимит stream-соединений котировок из тарифа (SPEC-0009)."""
    try:
        tariff = await services.users.get_user_tariff()
    except Exception as exc:  # noqa: BLE001 - лимит справочный, сбор не блокирует
        logger.warning("Не удалось получить тариф T-Invest: %s", exc)
        return
    for limit in getattr(tariff, "stream_limits", None) or []:
        streams = list(getattr(limit, "streams", None) or [])
        if any("MarketDataStream" in name for name in streams):
            logger.info(
                "Лимит stream-соединений котировок: %s (открыто сейчас: %s)",
                limit.limit,
                getattr(limit, "open", "?"),
            )


def make_connection_factory(config: AppConfig) -> ConnectionFactory:
    """Фабрика единственного стрим-соединения для :class:`StreamHub` (SPEC-0009).

    FIGI кэшируются между переподключениями и разрешаются заново только при
    ``resolve=True`` (старт, ролловер непрерывных фьючерсов) — G-4.
    """
    figis: dict[str, str] = {}
    not_found: list[str] = []
    limits_logged = False

    async def factory(instruments: Sequence[Instrument], resolve: bool) -> HubConnection:
        nonlocal limits_logged
        if not _has_sdk:
            raise RuntimeError(
                "t-tech-investments не установлен; стриминг рыночных данных недоступен"
            )
        client = AsyncClient(config.token)
        services = await client.__aenter__()
        try:
            if not limits_logged:
                await _log_stream_limits(services)
                limits_logged = True
            if resolve or not figis:
                resolved, missing = await _resolve_figis(services, list(instruments))
                figis.clear()
                figis.update(resolved)
                not_found[:] = missing
            if not figis:
                raise RuntimeError("ни один инструмент не разрешён в FIGI")
            if len(figis) > MAX_SUBSCRIPTIONS_PER_STREAM:
                raise ValueError(
                    f"{len(figis)} инструментов больше лимита подписок одного стрима "
                    f"({MAX_SUBSCRIPTIONS_PER_STREAM})"
                )
            routes: dict[str, str] = {}
            for name, figi in figis.items():
                if figi in routes:
                    logger.error(
                        "FIGI %s у %s и %s: сделки получит только %s",
                        figi,
                        routes[figi],
                        name,
                        routes[figi],
                    )
                    continue
                routes[figi] = name
            stream = services.create_market_data_stream()
            stream.subscribe(build_trades_subscription(list(routes)))
            stream.subscribe(build_info_subscription(list(routes)))
        except BaseException:
            # Иначе при каждом повторе открытия стрима утекал бы gRPC-канал.
            await client.__aexit__(None, None, None)  # type: ignore[no-untyped-call]
            raise

        async def teardown() -> None:
            try:
                stream.stop()
            finally:
                await client.__aexit__(None, None, None)  # type: ignore[no-untyped-call]

        return HubConnection(
            stream=stream, routes=routes, teardown=teardown, not_found=list(not_found)
        )

    return factory


async def _run_backfill(
    config: AppConfig, repos: dict[str, MarketDataRepository], **kwargs: Any
) -> None:
    """Запускает ``autoload.run_backfill`` с сервисами SDK, если он установлен."""
    if not _has_sdk:
        await autoload.run_backfill(config, repos, **kwargs)
        return
    sdk_client = AsyncClient(config.token)
    services = await sdk_client.__aenter__()
    try:
        await autoload.run_backfill(
            config,
            repos,
            market_data=services.market_data,
            instruments=services.instruments,
            kind_share=_SdkInstrumentType.INSTRUMENT_TYPE_SHARE,
            kind_futures=_SdkInstrumentType.INSTRUMENT_TYPE_FUTURES,
            # Истёкшие контракты нужны для прошедших дней непрерывных фьючерсов.
            futures_status=_SdkInstrumentStatus.INSTRUMENT_STATUS_ALL,
            **kwargs,
        )
    finally:
        await sdk_client.__aexit__(None, None, None)  # type: ignore[no-untyped-call]


async def run_maintenance(
    config: AppConfig,
    repos: dict[str, MarketDataRepository],
    *,
    gate: Any = None,
) -> None:
    """Обслуживание хранилища: догрузка/перекачка дней, затем архивация и очистка.

    Выполняется при старте и ежесуточно по расписанию ``config.archiver``.
    Ошибки этапов логируются и не прерывают работу коллекторов.
    """
    logger.info(
        "Проверка автозагрузки/догрузки истории (последние %d дней)",
        config.history_check_days,
    )
    try:
        await _run_backfill(config, repos, gate=gate)
    except Exception as exc:  # noqa: BLE001
        logger.error("Ошибка автозагрузки: %s", exc)

    logger.info("Сверка архиватора")
    try:
        await archiver_mod.run_archiver(config, repos)
    except Exception as exc:  # noqa: BLE001
        logger.error("Ошибка сверки архиватора: %s", exc)


async def run_dealer_cleanup(
    config: AppConfig,
    repos: dict[str, MarketDataRepository],
    *,
    gate: Any,
    lock: asyncio.Lock,
) -> None:
    """Однократная чистка хранилища от дилерских сделок (SPEC-0002 G-4).

    Отмечает все сохранённые дни и перекачивает их из архива с биржевым
    фильтром. Работает в фоне параллельно с коллекторами: тысячи файлов при
    лимите архива ~28/мин заняли бы часы. Делит ограничитель темпа с ночным
    обслуживанием и берёт ``lock`` на каждый день, чтобы не пересекаться с
    ним (архиватор пишет Parquet тех же дней).
    """
    for repo in repos.values():
        await asyncio.to_thread(autoload.schedule_dealer_cleanup, repo)
    for attempt in range(1, CLEANUP_MAX_PASSES + 1):
        pending = 0
        for repo in repos.values():
            pending += len(
                await asyncio.to_thread(autoload.refresh_days, config, repo, cleanup=True)
            )
        if not pending:
            logger.info("Чистка от дилерских сделок завершена")
            return
        logger.info("Чистка от дилерских сделок: проход %d, осталось дней %d", attempt, pending)
        try:
            await _run_backfill(config, repos, gate=gate, lock=lock, cleanup=True)
        except Exception as exc:  # noqa: BLE001
            logger.error("Ошибка чистки от дилерских сделок: %s", exc)
        await asyncio.sleep(CLEANUP_RETRY_SEC)
    logger.error(
        "Чистка от дилерских сделок не завершена за %d проходов; продолжится при следующем запуске",
        CLEANUP_MAX_PASSES,
    )


async def run_scheduler(config: AppConfig, job: Callable[[], Awaitable[None]]) -> None:
    """Бесконечно запускает ``job`` ежесуточно в ``run_hour_utc:run_minute_utc`` UTC."""
    while True:
        delay = archiver_mod.seconds_until_next_run(datetime.now(UTC), config.archiver)
        logger.info(
            "Следующее ночное обслуживание через %.0f с (%02d:%02d UTC)",
            delay,
            config.archiver.run_hour_utc,
            config.archiver.run_minute_utc,
        )
        await asyncio.sleep(delay)
        logger.info("Ночное обслуживание: старт")
        await job()


async def amain(config: AppConfig) -> int:
    repos: dict[str, MarketDataRepository] = {}
    for instrument in config.instruments:
        repos[instrument.storage_name] = MarketDataRepository(config.storage_root, instrument)

    archive_gate = autoload.archive_gate()
    maintenance_lock = asyncio.Lock()
    async with maintenance_lock:
        await run_maintenance(config, repos, gate=archive_gate)

    collectors = [
        DataCollector(config, instrument, repos[instrument.storage_name])
        for instrument in config.instruments
    ]
    # SPEC-0009: одно стрим-соединение на все инструменты (лимит T-Invest — 16).
    hub = StreamHub(collectors, make_connection_factory(config))

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
    hub_task = asyncio.create_task(hub.run())

    async def nightly() -> None:
        async with maintenance_lock:
            await run_maintenance(config, repos, gate=archive_gate)
        # Активный контракт непрерывного фьючерса мог смениться (экспирация).
        if any(c.instrument.is_continuous for c in collectors):
            hub.request_reconnect(resolve=True)

    scheduler_task = asyncio.create_task(run_scheduler(config, nightly))
    cleanup_task = asyncio.create_task(
        run_dealer_cleanup(config, repos, gate=archive_gate, lock=maintenance_lock)
    )
    background = [hub_task, scheduler_task, cleanup_task]
    if config.iss.enabled:
        # SPEC-0003: лента Мосбиржи — источник номеров сделок и ОИ по сделкам.
        iss_collector = IssCollector(config, IssTapeRepository(config.storage_root))
        background.append(asyncio.create_task(iss_collector.run()))

    try:
        await stop_event.wait()
    finally:
        logger.info("Запрошено корректное завершение; сбрасываем накопленные тики в SQLite")
        await hub.shutdown()
        # Сначала останавливаем стрим, затем коллекторы дописывают свои очереди.
        for task in background:
            task.cancel()
        await asyncio.gather(*background, return_exceptions=True)
        for task in collector_tasks:
            task.cancel()
        await asyncio.gather(*collector_tasks, return_exceptions=True)
        for collector in collectors:
            await collector.shutdown()
        logger.info("Все коллекторы опустошены; соединения с БД закрыты")
    return 0


def prevent_idle_sleep(config: AppConfig) -> subprocess.Popen[bytes] | None:
    """SPEC-0010: на macOS запрещает сон по простою, пока жив этот процесс.

    Пока Mac спит, сбор стоит целиком (2026-10-13: сон 08:08–08:21 МСК).
    ``caffeinate -i -w <pid>`` держит запрет ровно до выхода процесса, даже
    при аварийном завершении. Закрытие крышки ноутбука это не отменяет.
    """
    if sys.platform != "darwin" or not config.keep_awake:
        return None
    binary = shutil.which("caffeinate")
    if binary is None:
        logger.warning("caffeinate не найден — Mac может уснуть и остановить сбор")
        return None
    try:
        proc = subprocess.Popen([binary, "-i", "-w", str(os.getpid())])
    except OSError as exc:
        logger.warning("Не удалось запретить сон (caffeinate): %s", exc)
        return None
    logger.info("Сон по простою запрещён, пока работает процесс (caffeinate, PID %d)", proc.pid)
    return proc


def main() -> int:
    setup_logging()
    config_path = Path("config.local.yaml") if Path("config.local.yaml").exists() else CONFIG_PATH
    config = AppConfig.from_yaml(config_path)
    prevent_idle_sleep(config)
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
