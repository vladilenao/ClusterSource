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

Хаб же ведёт детекторы деградации SPEC-0004 уровня соединения: разрыв
(``stream_gap``), неподтверждённая подписка, отставание, тишина при идущих
торгах (по торговым статусам из того же соединения) и «мёртвое» соединение.
Сторож раз в ``WATCHDOG_SEC`` проверяет условия и помечает минуты.
"""

from __future__ import annotations

import asyncio
import logging
import statistics
from collections import deque
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from cluster_source.collector import DataCollector
from cluster_source.config import Instrument
from cluster_source.quality import (
    REASON_CONTRACT_ROLL,
    REASON_LATENCY,
    REASON_STREAM_GAP,
    REASON_SUBSCRIPTION_FAILED,
    is_normal_trading,
)

logger = logging.getLogger(__name__)

MAX_RETRY_DELAY = 60.0
RETRY_BASE = 2.0
#: Соединение, прожившее дольше, считается стабильным: задержка переподключения сбрасывается.
STABLE_STREAM_SEC = 60.0
#: Не больше стольких подписок в одном соединении (лимит T-Invest).
MAX_SUBSCRIPTIONS_PER_STREAM = 300
#: SPEC-0004: период сторожа, окно и пороги отставания, «мёртвое» соединение.
WATCHDOG_SEC = 5.0
LATENCY_WINDOW_SEC = 30.0
LATENCY_HIGH_SEC = 5.0
LATENCY_LOW_SEC = 3.0
DEAD_STREAM_SEC = 180.0


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
    """Держит одно соединение, раздаёт сделки и ведёт детекторы уровня соединения."""

    def __init__(
        self,
        collectors: Sequence[DataCollector],
        factory: ConnectionFactory,
        *,
        now: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        self._collectors = {c.instrument.storage_name: c for c in collectors}
        self._factory = factory
        self._now = now
        self._running = False
        self._reconnect_requested = False
        self._resolve_requested = True  # первое соединение разрешает FIGI
        self._unknown_routes: set[str] = set()
        self._routes: dict[str, str] = {}
        self._figis: dict[str, str] = {}  # storage_name -> FIGI (для ролловера)
        self._connected = False
        self._last_frame_at: datetime | None = None
        self._lags: deque[tuple[datetime, float]] = deque()
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
        await self._begin_startup_gap()
        watchdog = asyncio.create_task(self._watchdog())
        try:
            await self._connection_loop()
        finally:
            watchdog.cancel()

    async def _connection_loop(self) -> None:
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
                if self._running:  # штатная остановка — не деградация
                    self._on_lost()
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

    # ------------------------------------------------------------------ #
    # Открытие, обрыв, кадры
    # ------------------------------------------------------------------ #
    async def _begin_startup_gap(self) -> None:
        """SPEC-0004 ``stream_gap`` с последней сохранённой сделки (не раньше начала суток)."""
        now = self._now()
        midnight = now.replace(hour=0, minute=0, second=0, microsecond=0)
        for collector in self._collectors.values():
            try:
                last = await asyncio.to_thread(collector.last_stored_tick_time)
            except Exception as exc:  # noqa: BLE001 - хранилище недоступно: от старта
                logger.warning("Нет времени последней сделки %s: %s", collector.instrument.id, exc)
                last = None
            since = max(last, midnight) if last is not None else midnight
            collector.quality.begin(REASON_STREAM_GAP, since, now)

    def _on_opened(self, conn: HubConnection) -> None:
        now = self._now()
        subscribed = sorted(set(conn.routes.values()))
        for name in conn.not_found:
            logger.warning("Инструмент %s не найден у провайдера — не собирается", name)
        figis = {name: key for key, name in conn.routes.items()}
        for name in subscribed:
            collector = self._collectors[name]
            # SPEC-0009 G-2: сделки до (пере)открытия не получены у всех инструментов.
            collector.on_stream_opened()
            collector.quality.end(REASON_SUBSCRIPTION_FAILED, now)
            previous = self._figis.get(name)
            if previous is not None and previous != figis[name]:
                collector.quality.mark(REASON_CONTRACT_ROLL, now)  # SPEC-0004
                logger.info("Ролловер %s: %s -> %s", name, previous, figis[name])
        self._figis.update(figis)
        self._routes = dict(conn.routes)
        self._connected = True
        self._last_frame_at = now
        self.failed_subscriptions.clear()
        logger.info(
            "Стрим сделок открыт: одно соединение, %d инструментов (%s)",
            len(subscribed),
            ", ".join(subscribed),
        )

    def _on_lost(self) -> None:
        """SPEC-0004 ``stream_gap``: с последнего кадра до подтверждения новой подписки."""
        now = self._now()
        since = self._last_frame_at or now
        self._connected = False
        self._lags.clear()
        for collector in self._collectors.values():
            collector.listening_since = None
            collector.quality.begin(REASON_STREAM_GAP, since, now)
            if collector.quality.is_active(REASON_LATENCY):
                collector.quality.end(REASON_LATENCY, now)

    def _end_gap(self, name: str) -> None:
        collector = self._collectors.get(name)
        if collector is None:
            return
        if collector.listening_since is None:
            collector.listening_since = self._now()
        if collector.quality.is_active(REASON_STREAM_GAP):
            collector.quality.end(REASON_STREAM_GAP, self._now())

    async def _pump(self, conn: HubConnection) -> None:
        async for item in conn.stream:
            self._last_frame_at = self._now()
            if not self._running or self._reconnect_requested:
                return
            response = getattr(item, "subscribe_trades_response", None)
            if response is not None:
                self._check_subscriptions(response, conn.routes)
            info = getattr(item, "subscribe_info_response", None)
            if info is not None:
                subs = list(getattr(info, "info_subscriptions", None) or [])
                ok = sum(1 for s in subs if subscription_ok(getattr(s, "subscription_status", "")))
                logger.info("Подписка на торговые статусы подтверждена: %d из %d", ok, len(subs))
            status = getattr(item, "trading_status", None)
            if status is not None:
                self._on_trading_status(status, conn.routes)
            trade = getattr(item, "trade", None)
            if trade is not None:
                await self._route(trade, conn.routes)

    def _resolve_route(self, payload: Any, routes: dict[str, str]) -> str | None:
        for key in (getattr(payload, "figi", None), getattr(payload, "instrument_uid", None)):
            if key and str(key) in routes:
                return routes[str(key)]
        return None

    async def _route(self, trade: Any, routes: dict[str, str]) -> None:
        name = self._resolve_route(trade, routes)
        if name is None:
            figi = str(getattr(trade, "figi", "") or getattr(trade, "instrument_uid", ""))
            if figi not in self._unknown_routes:
                self._unknown_routes.add(figi)
                logger.warning("Сделка по неподписанному инструменту %s — пропущена", figi)
            return
        self._observe_lag(trade)
        self._end_gap(name)  # сделка доказывает, что подписка работает
        await self._collectors[name].deliver(trade)

    def _on_trading_status(self, status: Any, routes: dict[str, str]) -> None:
        name = self._resolve_route(status, routes)
        if name is None:
            return
        moment = getattr(status, "time", None)
        since = moment.astimezone(UTC) if isinstance(moment, datetime) else None
        self._collectors[name].set_trading_status(getattr(status, "trading_status", None), since)

    def _observe_lag(self, trade: Any) -> None:
        moment = getattr(trade, "time", None)
        if not isinstance(moment, datetime):
            return
        moment = moment if moment.tzinfo else moment.replace(tzinfo=UTC)
        now = self._now()
        self._lags.append((now, (now - moment).total_seconds()))

    def _check_subscriptions(self, response: Any, routes: dict[str, str]) -> None:
        """SPEC-0009 G-3: неподтверждённая подписка не считается собираемой."""
        subs = list(getattr(response, "trade_subscriptions", None) or [])
        ok = sum(1 for sub in subs if subscription_ok(getattr(sub, "subscription_status", "")))
        logger.info("Подписка на сделки подтверждена: %d из %d", ok, len(subs))
        now = self._now()
        for sub in subs:
            status = getattr(sub, "subscription_status", "")
            key = str(getattr(sub, "figi", "") or getattr(sub, "instrument_uid", ""))
            storage = routes.get(key, key)
            if subscription_ok(status):
                self._end_gap(storage)
                continue
            name = getattr(status, "name", None) or str(status)
            self.failed_subscriptions.add(storage)
            logger.error("STREAM_SUBSCRIPTION_FAILED %s (%s): статус %s", storage, key, name)
            if storage in self._collectors:
                collector = self._collectors[storage]
                collector.on_stream_opened()
                collector.quality.begin(REASON_SUBSCRIPTION_FAILED, now, now)
                self._end_gap(storage)

    # ------------------------------------------------------------------ #
    # Сторож SPEC-0004
    # ------------------------------------------------------------------ #
    async def _watchdog(self) -> None:
        while True:
            await asyncio.sleep(WATCHDOG_SEC)
            try:
                self.check(self._now())
            except Exception as exc:  # noqa: BLE001 - сторож не должен падать
                logger.error("Ошибка сторожа качества: %s", exc)

    def check(self, now: datetime) -> None:
        """Один проход сторожа: отставание, тишина, мёртвое соединение, отметки минут."""
        if self._connected:
            self._check_latency(now)
            for collector in self._collectors.values():
                if collector.instrument.storage_name in self._routes.values():
                    collector.check_silence(now)
            self._check_dead_stream(now)
        for collector in self._collectors.values():
            collector.close_minutes(now)
            collector.quality.tick(now)

    def _check_latency(self, now: datetime) -> None:
        while self._lags and (now - self._lags[0][0]).total_seconds() > LATENCY_WINDOW_SEC:
            self._lags.popleft()
        if not self._lags:
            return
        median = statistics.median(lag for _, lag in self._lags)
        for collector in self._collectors.values():
            active = collector.quality.is_active(REASON_LATENCY)
            if median > LATENCY_HIGH_SEC and not active:
                collector.quality.begin(REASON_LATENCY, now, now)
            elif median < LATENCY_LOW_SEC and active:
                collector.quality.end(REASON_LATENCY, now)

    def _check_dead_stream(self, now: datetime) -> None:
        if self._last_frame_at is None or self._reconnect_requested:
            return
        trading = any(is_normal_trading(c.trading_status) for c in self._collectors.values())
        silent = (now - self._last_frame_at).total_seconds()
        if trading and silent > DEAD_STREAM_SEC:
            logger.error("Стрим сделок мёртв: нет кадров %.0f с — переподключение", silent)
            self.request_reconnect()

    @staticmethod
    async def _teardown(conn: HubConnection) -> None:
        if conn.teardown is not None:
            try:
                await conn.teardown()
            except Exception as exc:  # pragma: no cover - resilience
                logger.warning("Ошибка закрытия стрима: %s", exc)
