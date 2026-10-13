"""Одно стрим-соединение T-Invest на все инструменты (SPEC-0009).

Лимит T-Invest — 16 активных stream-соединений сервиса котировок на грейдах
1–4 (64 на грейде 5), до 300 подписок в одном соединении. Соединение на
каждый инструмент превышало лимит в разы: сервер сбрасывал лишние
(``RST_STREAM`` с кодом 8), коллекторы переподключались по кругу и теряли
сделки. :class:`StreamHub` держит одно соединение с подпиской на сделки всех
инструментов и раздаёт сделки в :class:`~cluster_source.collector.DataCollector`
по FIGI / UID инструмента.

Соединение открывает инжектируемая фабрика (SDK — только в ``main.py``).
FIGI разрешаются при старте и при запросе переподключения с ``resolve=True``
(ролловер непрерывных фьючерсов), но не при каждом переподключении (G-4).
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass, field
from typing import Any

from cluster_source.collector import DataCollector
from cluster_source.config import Instrument

logger = logging.getLogger(__name__)

MAX_RETRY_DELAY = 60.0
RETRY_BASE = 2.0
#: Соединение, прожившее дольше, считается стабильным: задержка переподключения сбрасывается.
STABLE_STREAM_SEC = 60.0
#: Не больше стольких подписок в одном соединении (лимит T-Invest).
MAX_SUBSCRIPTIONS_PER_STREAM = 300


@dataclass(frozen=True)
class HubConnection:
    """Открытое соединение: стрим, маршруты сделок и закрытие.

    ``routes`` — ``{FIGI или instrument_uid: storage_name инструмента}`` для
    всех подписанных инструментов. ``teardown`` вызывается ровно один раз.
    """

    stream: Any
    routes: dict[str, str]
    teardown: Callable[[], Awaitable[None]] | None = None
    #: Инструменты, которых нет у провайдера: в подписку не вошли.
    not_found: list[str] = field(default_factory=list)


ConnectionFactory = Callable[[Sequence[Instrument], bool], Awaitable[HubConnection]]
"""Открывает соединение для инструментов; ``True`` — заново разрешить FIGI."""


def subscription_ok(status: Any) -> bool:
    """Успешен ли статус подписки (``SUBSCRIPTION_STATUS_SUCCESS`` = 1; SDK — ``IntEnum``)."""
    if isinstance(status, int):
        return int(status) == 1
    return str(status).strip().upper() in {"1", "SUBSCRIPTION_STATUS_SUCCESS"}


def _next_delay(delay: float) -> float:
    """Следующая задержка переподключения: 2, 4, 8, … не более ``MAX_RETRY_DELAY``."""
    return min(max(delay * 2, RETRY_BASE), MAX_RETRY_DELAY)


class StreamHub:
    """Держит одно соединение и раздаёт сделки коллекторам инструментов."""

    def __init__(self, collectors: Sequence[DataCollector], factory: ConnectionFactory) -> None:
        self._collectors = {c.instrument.storage_name: c for c in collectors}
        self._factory = factory
        self._running = False
        self._reconnect_requested = False
        self._resolve_requested = True  # первое соединение разрешает FIGI
        self._unknown_routes: set[str] = set()
        self.failed_subscriptions: set[str] = set()

    def request_reconnect(self, resolve: bool = False) -> None:
        """Просит переоткрыть соединение; ``resolve`` — заново разрешить FIGI.

        Срабатывает на следующем кадре стрима (сделка или ping сервера).
        Нужен непрерывным фьючерсам: после экспирации активный контракт меняется.
        """
        self._resolve_requested = self._resolve_requested or resolve
        self._reconnect_requested = True

    async def shutdown(self) -> None:
        self._running = False

    async def run(self) -> None:
        """Держит соединение открытым; переподключается с экспоненциальной задержкой."""
        self._running = True
        instruments = [c.instrument for c in self._collectors.values()]
        loop = asyncio.get_running_loop()
        delay = 0.0
        while self._running:
            resolve = self._resolve_requested
            self._reconnect_requested = False
            try:
                conn = await self._factory(instruments, resolve)
            except Exception as exc:  # noqa: BLE001 - сеть, gRPC, резолюция
                delay = _next_delay(delay)
                logger.error("Не удалось открыть стрим сделок: %s; повтор через %.1f с", exc, delay)
                await asyncio.sleep(delay)
                continue
            self._resolve_requested = False
            self._on_opened(conn)
            started = loop.time()
            try:
                await self._pump(conn)
                reason = (
                    "переподключение по запросу"
                    if self._reconnect_requested
                    else "стрим закрыт сервером"
                )
            except Exception as exc:  # noqa: BLE001 - обрыв соединения
                reason = f"ошибка стрима: {exc}"
            finally:
                await self._teardown(conn)

            if not self._running:
                return
            if self._reconnect_requested:
                delay = 0.0
                logger.info("Стрим сделок переоткрывается: %s", reason)
                continue
            if loop.time() - started >= STABLE_STREAM_SEC:
                delay = 0.0
            delay = _next_delay(delay)
            logger.warning("Стрим сделок прерван (%s); переподключение через %.1f с", reason, delay)
            await asyncio.sleep(delay)

    def _on_opened(self, conn: HubConnection) -> None:
        subscribed = sorted(set(conn.routes.values()))
        for name in conn.not_found:
            logger.warning("Инструмент %s не найден у провайдера — не собирается", name)
        for name in subscribed:
            # SPEC-0009 G-2: сделки до (пере)открытия не получены у всех инструментов.
            self._collectors[name].on_stream_opened()
        self.failed_subscriptions.clear()
        logger.info(
            "Стрим сделок открыт: одно соединение, %d инструментов (%s)",
            len(subscribed),
            ", ".join(subscribed),
        )

    async def _pump(self, conn: HubConnection) -> None:
        async for item in conn.stream:
            if not self._running or self._reconnect_requested:
                return
            response = getattr(item, "subscribe_trades_response", None)
            if response is not None:
                self._check_subscriptions(response, conn.routes)
            trade = getattr(item, "trade", None)
            if trade is not None:
                await self._route(trade, conn.routes)

    async def _route(self, trade: Any, routes: dict[str, str]) -> None:
        key = next(
            (
                str(k)
                for k in (getattr(trade, "figi", None), getattr(trade, "instrument_uid", None))
                if k and str(k) in routes
            ),
            None,
        )
        if key is None:
            figi = str(getattr(trade, "figi", "") or getattr(trade, "instrument_uid", ""))
            if figi not in self._unknown_routes:
                self._unknown_routes.add(figi)
                logger.warning("Сделка по неподписанному инструменту %s — пропущена", figi)
            return
        await self._collectors[routes[key]].deliver(trade)

    def _check_subscriptions(self, response: Any, routes: dict[str, str]) -> None:
        """SPEC-0009 G-3: неподтверждённая подписка не считается собираемой."""
        subs = list(getattr(response, "trade_subscriptions", None) or [])
        ok = sum(1 for sub in subs if subscription_ok(getattr(sub, "subscription_status", "")))
        logger.info("Подписка на сделки подтверждена: %d из %d", ok, len(subs))
        for sub in subs:
            status = getattr(sub, "subscription_status", "")
            if subscription_ok(status):
                continue
            name = getattr(status, "name", None) or str(status)
            key = str(getattr(sub, "figi", "") or getattr(sub, "instrument_uid", ""))
            storage = routes.get(key, key)
            self.failed_subscriptions.add(storage)
            logger.error("STREAM_SUBSCRIPTION_FAILED %s (%s): статус %s", storage, key, name)
            if storage in self._collectors:
                self._collectors[storage].on_stream_opened()

    @staticmethod
    async def _teardown(conn: HubConnection) -> None:
        if conn.teardown is not None:
            try:
                await conn.teardown()
            except Exception as exc:  # pragma: no cover - resilience
                logger.warning("Ошибка закрытия стрима: %s", exc)
