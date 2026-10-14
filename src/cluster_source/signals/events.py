"""Конверт событий Redis Stream ``cluster.events`` (SPEC-0008, AsyncAPI
``docs/contracts/asyncapi/cluster-source-events.asyncapi.yaml``).

Конверт повторяет ``market.events`` робота: ``schema_version``, детерминированный
``event_id`` (sha256), ``source``, ``event_type``, ``ticker``, ``timestamp`` —
время рынка, ``published_at`` — время Redis перед ``XADD`` (добавляет
публикатор). Поля Redis Stream — строки, объекты — JSON-строки.
"""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from typing import Any

SCHEMA_VERSION = 1
SOURCE = "cluster_source"

CATEGORY_SIGNAL = "signal"
CATEGORY_RESULT = "signal_result"
CATEGORY_QUALITY = "data_quality"

SIGNAL_CONFIRMED = "signal_confirmed"
SIGNAL_REVOKED = "signal_revoked"
DATA_DEGRADED = "data_degraded"
DATA_RESTORED = "data_restored"

OUTCOME_VERIFIED = "VERIFIED"
OUTCOME_REPAIRED = "REPAIRED"
REASON_GAP = "gap"
REASON_REPAIRED_CHANGED = "repaired_changed_result"
REASON_EXTRA_TRADES = "extra_trades"
REASON_UNAVAILABLE = "verification_unavailable"

Event = dict[str, Any]


def iso(moment: datetime) -> str:
    """UTC ISO 8601 с микросекундами и ``Z``, как в ``market.events``."""
    moment = moment if moment.tzinfo else moment.replace(tzinfo=UTC)
    return moment.astimezone(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")


def parse_iso(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)


def event_id(category: str, event_type: str, ticker: str, key: str) -> str:
    """Ключ дедупликации: sha256 от ``cluster_source/1/{category}/{type}/{ticker}/{key}``."""
    identity = f"{SOURCE}/{SCHEMA_VERSION}/{category}/{event_type}/{ticker}/{key}"
    return hashlib.sha256(identity.encode()).hexdigest()


def envelope(
    category: str, event_type: str, ticker: str, secid: str, timestamp: datetime, key: str
) -> Event:
    return {
        "schema_version": SCHEMA_VERSION,
        "event_id": event_id(category, event_type, ticker, key),
        "source": SOURCE,
        "category": category,
        "event_type": event_type,
        "ticker": ticker,
        "secid": secid,
        "timestamp": iso(timestamp),
    }


def result_event(
    signal: Event, status_type: str, moment: datetime, *, outcome: str | None, reason: str | None
) -> Event:
    """Итог проверки сигнала: ``signal_confirmed`` (``outcome``) или ``signal_revoked``."""
    event = envelope(
        CATEGORY_RESULT,
        status_type,
        str(signal["ticker"]),
        str(signal["secid"]),
        moment,
        str(signal["signal_id"]),
    )
    event["signal_id"] = signal["signal_id"]
    if outcome is not None:
        event["outcome"] = outcome
    if reason is not None:
        event["reason"] = reason
    return event


def quality_event(
    ticker: str, secid: str, moment: datetime, state: str, reasons: list[str]
) -> Event:
    """Смена качества данных инструмента (SPEC-0004): ``data_degraded`` / ``data_restored``."""
    kind = DATA_DEGRADED if state == "DEGRADED" else DATA_RESTORED
    event = envelope(
        CATEGORY_QUALITY, kind, ticker, secid, moment, f"{iso(moment)}/{','.join(reasons)}"
    )
    if kind == DATA_DEGRADED:
        event["reasons"] = reasons
    return event


def to_fields(event: Event) -> dict[str, str]:
    """Поля Redis Stream: строки; объекты и массивы — JSON, логические — ``true``/``false``."""
    out: dict[str, str] = {}
    for key, value in event.items():
        if isinstance(value, dict | list):
            out[key] = json.dumps(value, ensure_ascii=False)
        elif isinstance(value, bool):
            out[key] = "true" if value else "false"
        else:
            out[key] = str(value)
    return out


def dumps(event: Event) -> str:
    return json.dumps(event, ensure_ascii=False, sort_keys=True)
