"""Публикация событий в Redis Stream ``cluster.events`` (SPEC-0008 G-5).

Механика повторяет ``market.events`` робота:

* событие уже лежит в outbox (``SignalRepository``) — сигнал и итог пишет
  движок, события качества переносятся сюда из ``quality_events``
  инструментов по курсору (таблица — тоже outbox);
* перед ``XADD`` берётся ``Redis TIME`` → ``published_at``; повтор несёт тот
  же ``event_id``;
* ошибка Redis — повтор не раньше чем через 1 с (с удвоением до 60 с), порядок
  событий сохраняется; сбор и детекторы Redis не ждут;
* раз в сутки — точный ``XTRIM MINID`` (время Redis − ``retention_days``),
  ``MAXLEN`` не применяется; из outbox удаляется доставленное старше срока.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
from collections.abc import Callable, Mapping
from datetime import UTC, datetime, timedelta
from typing import Any, Protocol, cast

from cluster_source.config import AppConfig
from cluster_source.database import MarketDataRepository, SignalRepository
from cluster_source.signals import events as ev

logger = logging.getLogger(__name__)

PASSWORD_ENV = "CLUSTER_SOURCE_REDIS_PASSWORD"
PUBLISH_INTERVAL_SEC = 1.0
MAX_BACKOFF_SEC = 60.0
TRIM_EVERY = timedelta(days=1)
LOG_FAILURE_EVERY = timedelta(minutes=1)
QUALITY_CURSOR = "quality_rowid:"


class RedisLike(Protocol):
    async def time(self) -> Any: ...
    async def xadd(self, name: str, fields: dict[str, str]) -> Any: ...
    async def xtrim(self, name: str, *, minid: str, approximate: bool) -> Any: ...


def redis_client(config: AppConfig) -> RedisLike:
    """Клиент ``redis.asyncio``; пароль — только из окружения (SPEC-0008)."""
    from redis.asyncio import Redis

    settings = config.signals.redis
    client = Redis(
        host=settings.host,
        port=settings.port,
        db=settings.db,
        password=os.environ.get(PASSWORD_ENV) or None,
        socket_timeout=5,
        socket_connect_timeout=5,
    )
    return cast(RedisLike, client)


def _redis_moment(value: Any) -> datetime:
    seconds, micros = value
    return datetime.fromtimestamp(int(seconds) + int(micros) / 1_000_000, tz=UTC)


class EventPublisher:
    """Outbox → Redis Stream; события качества — из ``quality_events`` по курсору."""

    def __init__(
        self,
        config: AppConfig,
        signal_repo: SignalRepository,
        repos: Mapping[str, MarketDataRepository],
        secid_of: Callable[[str], str | None],
        client_factory: Callable[[], RedisLike] | None = None,
        *,
        now: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        self._config = config
        self._settings = config.signals.redis
        self._repo = signal_repo
        self._repos = repos
        self._secid_of = secid_of
        self._factory = client_factory or (lambda: redis_client(config))
        self._client: RedisLike | None = None
        self._now = now
        self._last_trim: datetime | None = None
        self._last_failure_log: datetime | None = None
        self._ids = {i.storage_name: i.id for i in config.instruments}

    async def run(self) -> None:
        while True:
            try:
                await self.run_once()
            except Exception as exc:  # noqa: BLE001 - публикатор не должен останавливаться
                logger.error("Ошибка публикатора событий: %s", exc)
            await asyncio.sleep(PUBLISH_INTERVAL_SEC)

    async def run_once(self) -> None:
        await self.collect_quality_events()
        if await self.publish_due():
            await self.trim_if_due()

    # ------------------------------------------------------------------ #
    # События качества (SPEC-0004) → outbox
    # ------------------------------------------------------------------ #
    async def collect_quality_events(self) -> None:
        """Переносит новые переходы ``quality_events`` в outbox по курсору ``rowid``.

        При первом запуске курсор ставится на конец журнала: история не
        переигрывается. Перенос и сдвиг курсора — одна транзакция.
        """
        for name, repo in self._repos.items():
            key = f"{QUALITY_CURSOR}{name}"
            stored = await asyncio.to_thread(self._repo.get_meta, key)
            if stored is None:
                last = await asyncio.to_thread(repo.max_quality_event_rowid)
                await asyncio.to_thread(self._repo.set_meta, key, str(last))
                continue
            rows = await asyncio.to_thread(repo.read_quality_events, int(stored))
            if not rows:
                continue
            secid = self._secid_of(name) or self._ids.get(name, name)
            events = []
            for _rowid, ts, state, reasons in rows:
                event = ev.quality_event(
                    name,
                    secid,
                    datetime.fromisoformat(ts),
                    state,
                    [r for r in reasons.split(",") if r],
                )
                events.append((str(event["event_id"]), ev.dumps(event)))
            await asyncio.to_thread(
                self._repo.enqueue_events,
                events,
                ev.iso(self._now()),
                {key: str(rows[-1][0])},
            )

    # ------------------------------------------------------------------ #
    # Outbox → Redis
    # ------------------------------------------------------------------ #
    async def publish_due(self) -> bool:
        """Публикует готовые события по порядку; ``False`` — Redis недоступен."""
        due = await asyncio.to_thread(self._repo.due_events, ev.iso(self._now()))
        for seq, _event_id, payload, attempts in due:
            try:
                client = self._get_client()
                event = json.loads(payload)
                event["published_at"] = ev.iso(_redis_moment(await client.time()))
                await client.xadd(self._settings.stream, ev.to_fields(event))
            except Exception as exc:  # noqa: BLE001 - любая ошибка Redis — повтор позже
                await self._defer(seq, attempts, exc)
                return False
            await asyncio.to_thread(self._repo.mark_delivered, seq, ev.iso(self._now()))
        return True

    def _get_client(self) -> RedisLike:
        if self._client is None:
            self._client = self._factory()
        return self._client

    async def _defer(self, seq: int, attempts: int, exc: Exception) -> None:
        now = self._now()
        delay = min(MAX_BACKOFF_SEC, max(1.0, 2.0**attempts))
        await asyncio.to_thread(
            self._repo.defer_event, seq, str(exc), ev.iso(now + timedelta(seconds=delay))
        )
        if self._last_failure_log is None or now - self._last_failure_log >= LOG_FAILURE_EVERY:
            backlog = await asyncio.to_thread(self._repo.outbox_backlog)
            logger.warning(
                "EVENTS_PUBLISH_FAILED: %s; в outbox %d событий, повтор через %.0f с",
                exc,
                backlog,
                delay,
            )
            self._last_failure_log = now

    async def trim_if_due(self) -> None:
        """Раз в сутки: ``XTRIM MINID`` по времени Redis и чистка outbox старше срока."""
        now = self._now()
        if self._last_trim is not None and now - self._last_trim < TRIM_EVERY:
            return
        try:
            client = self._get_client()
            current = _redis_moment(await client.time())
            cutoff = current - timedelta(days=self._settings.retention_days)
            await client.xtrim(
                self._settings.stream,
                minid=f"{int(cutoff.timestamp() * 1000)}-0",
                approximate=False,
            )
            await asyncio.to_thread(self._repo.purge_before, ev.iso(cutoff))
        except Exception as exc:  # noqa: BLE001
            logger.warning("Не удалось выполнить retention %s: %s", self._settings.stream, exc)
            return
        self._last_trim = now
