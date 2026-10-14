"""SPEC-0008: доставка событий в Redis Stream cluster.events."""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
import yaml
from jsonschema import Draft202012Validator, FormatChecker
from test_signals import NORMS, SBER, T0, candidate, cfg, make_engine, tick  # noqa: F401 - фикстура

from cluster_source.config import AppConfig
from cluster_source.signals import events as ev
from cluster_source.signals.detectors import Candidate, VolumeSpikeDetector
from cluster_source.signals.levels import Bar, absorptions, stacked_imbalances
from cluster_source.signals.publisher import EventPublisher

UTC = UTC
CONTRACT = (
    Path(__file__).resolve().parents[1]
    / "docs/contracts/asyncapi/cluster-source-events.asyncapi.yaml"
)


class FakeRedis:
    """Redis Streams в памяти: TIME, XADD, XTRIM; ``down`` — Redis недоступен."""

    def __init__(self) -> None:
        self.stream: list[dict[str, str]] = []
        self.trims: list[str] = []
        self.down = False

    async def time(self) -> tuple[int, int]:
        if self.down:
            raise ConnectionError("Connection refused")
        return (int(T0.timestamp()) + 3600, 250000)

    async def xadd(self, name: str, fields: dict[str, str]) -> bytes:
        if self.down:
            raise ConnectionError("Connection refused")
        assert name == "cluster.events"
        assert all(isinstance(v, str) for v in fields.values())  # поля Stream — строки
        self.stream.append(fields)
        return f"{len(self.stream)}-0".encode()

    async def xtrim(self, name: str, *, minid: str, approximate: bool) -> int:
        assert approximate is False  # точный XTRIM MINID, без MAXLEN
        self.trims.append(minid)
        return 0


def publisher(cfg: AppConfig, signal_repo: Any, repos: Any, redis: FakeRedis,  # noqa: F811
              now: list[datetime]) -> EventPublisher:  # fmt: skip
    return EventPublisher(
        cfg, signal_repo, repos, lambda _n: None, lambda: redis, now=lambda: now[0]
    )


# --------------------------------------------------------------------------- #
async def test_outbox_survives_redis_outage(cfg: AppConfig) -> None:  # noqa: F811
    """SPEC-0008 A-6 (G-5): Redis недоступен — события ждут; потом уходят по порядку
    с тем же event_id; итог — после сигнала."""
    now = [T0 + timedelta(seconds=6)]
    engine, repos, signal_repo = make_engine(cfg, now)
    redis = FakeRedis()
    pub = publisher(cfg, signal_repo, repos, redis, now)
    await engine.emit("SBER", candidate())
    redis.down = True
    assert await pub.publish_due() is False
    assert redis.stream == [] and signal_repo.outbox_backlog() == 1
    assert signal_repo.due_events(ev.iso(now[0])) == []  # повтор не раньше чем через 1 с

    now[0] = T0 + timedelta(minutes=21)  # срок проверки: итог тоже в outbox
    await engine.verify_once()
    redis.down = False
    assert await pub.publish_due() is True
    assert signal_repo.outbox_backlog() == 0
    signal, result = redis.stream
    assert (signal["category"], result["category"]) == ("signal", "signal_result")
    assert result["signal_id"] == signal["event_id"] == signal["signal_id"]
    assert result["reason"] == "verification_unavailable"
    assert signal["published_at"].endswith("Z")
    assert json.loads(signal["payload"])["size_unit"] == "rub"
    assert signal["bar_closed"] == "false"

    stored = signal_repo.read_signals("2000-01-01", "2100-01-01").iloc[0]
    assert json.loads(stored["event_json"])["event_id"] == signal["event_id"]  # тот же id

    await pub.trim_if_due()
    assert redis.trims and redis.trims[0].endswith("-0")


def _validator(message: str) -> Draft202012Validator:
    doc = yaml.safe_load(CONTRACT.read_text(encoding="utf-8"))
    schema = {"$ref": f"#/components/schemas/{message}", "components": doc["components"]}
    return Draft202012Validator(schema, format_checker=FormatChecker())


def _published(event: ev.Event) -> ev.Event:
    return {**event, "published_at": ev.iso(T0 + timedelta(hours=1))}


async def test_messages_match_asyncapi_schema(cfg: AppConfig) -> None:  # noqa: F811
    """SPEC-0008 A-7 (G-1): сигналы, итоги и события качества соответствуют AsyncAPI."""
    engine, _repos, _signal_repo = make_engine(cfg, [T0])
    large = engine.build_signal("SBER", candidate())
    spike_det = VolumeSpikeDetector(SBER, NORMS, cfg.signals)
    spike_det.on_tick(tick(T0, 900, "BUY"), T0)
    (spike_candidate,) = spike_det.flush(T0 + timedelta(minutes=2))
    spike = engine.build_signal("SBER", spike_candidate)
    fut = engine.build_signal("Si_CONTINUOUS", candidate())
    assert fut["secid"] == "SiZ6" and fut["baseline_quality"] == "mixed"

    levels = []
    bar = Bar(T0, 0.01)
    for price, volume, side in [(300.05, 10, "BUY"), (300.00, 700, "SELL"), (300.03, 20, "BUY"),
                                (300.01, 60, "BUY"), (300.02, 40, "BUY")]:  # fmt: skip
        bar.add(price, volume, side)
    for event_type, direction, payload in [*absorptions(bar, 500.0, 2),
                                           *stacked_imbalances(bar, 3.0, 2, 10.0)]:  # fmt: skip
        levels.append(engine.build_signal("SBER", Candidate(
            event_type, "level", direction, "5m", T0, T0 + timedelta(minutes=5), True,
            T0 + timedelta(minutes=5), "", {**payload, "regime": "main"})))  # fmt: skip
    assert {e["event_type"] for e in levels} == {"absorption", "stacked_imbalance"}

    signal_schema = _validator("SignalEvent")
    for event in (large, spike, fut, *levels):
        signal_schema.validate(_published(event))
    with pytest.raises(Exception):  # noqa: B017 - любая ошибка схемы
        signal_schema.validate(_published({**large, "class": "level"}))  # large_aggressor — fast

    moment = T0 + timedelta(minutes=18)
    _validator("SignalConfirmedEvent").validate(
        _published(ev.result_event(large, ev.SIGNAL_CONFIRMED, moment, outcome="VERIFIED",
                                   reason=None))
    )  # fmt: skip
    _validator("SignalRevokedEvent").validate(
        _published(ev.result_event(large, ev.SIGNAL_REVOKED, moment, outcome=None, reason="gap"))
    )
    _validator("DataDegradedEvent").validate(
        _published(ev.quality_event("SBER", "SBER", T0, "DEGRADED", ["stream_silence"]))
    )
    _validator("DataRestoredEvent").validate(
        _published(ev.quality_event("SBER", "SBER", T0, "OK", []))
    )
    fields = ev.to_fields(_published(large))
    assert json.loads(fields["baseline_days"]) == {"final": 20, "archive": 0}


async def test_quality_events_published_by_cursor(cfg: AppConfig) -> None:  # noqa: F811
    """SPEC-0008 A-8: события качества — из quality_events по курсору, без повторов и
    без переигрывания истории."""
    now = [T0]
    _engine, repos, signal_repo = make_engine(cfg, now)
    redis = FakeRedis()
    pub = publisher(cfg, signal_repo, repos, redis, now)
    repo = repos["SBER"]
    repo.save_quality([], [(T0.isoformat(), "DEGRADED", "stream_gap")])  # история до старта
    await pub.run_once()
    assert redis.stream == []

    t1 = T0 + timedelta(minutes=5)
    repo.save_quality([], [(t1.isoformat(), "DEGRADED", "latency,stream_silence")])
    repo.save_quality([], [((t1 + timedelta(minutes=1)).isoformat(), "OK", "")])
    await pub.run_once()
    await pub.run_once()  # повтор не дублирует
    assert [(m["event_type"], m.get("reasons")) for m in redis.stream] == [
        ("data_degraded", '["latency", "stream_silence"]'),
        ("data_restored", None),
    ]
    assert redis.stream[0]["timestamp"] == ev.iso(t1)
    assert {m["category"] for m in redis.stream} == {"data_quality"}
