"""Активный контракт непрерывного фьючерса: ролл по объёму (SPEC-0014).

Решение на календарный день D принимается один раз и хранится в таблице
``active_contract`` хранилища непрерывного инструмента. Стрим, догрузка,
сверка, FINAL и читатель берут контракт дня отсюда (G-1).

Торговый день календарного дня D (``trading_day_of``) — первый будний
торговый день по календарю ISS не раньше D: сессии выходных и праздников
относятся к следующему торговому дню (как в FINAL, SPEC-0006), поэтому правила
для них считаются по этому торговому дню и один торговый день не делится между
контрактами.
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta

from cluster_source.autoload import REFRESH_ROLL_RULE
from cluster_source.config import FuturesMeta, Instrument, nearest_contract
from cluster_source.contracts import contract_of
from cluster_source.database import IssTapeRepository, MarketDataRepository
from cluster_source.iss import KIND_FEED, STATUS_INCOMPLETE
from cluster_source.schedule import is_trading_day

logger = logging.getLogger(__name__)

__all__ = ["Decision", "LocalVolumes", "contract_of", "decide", "ensure_decisions"]

REASON_INITIAL = "initial"
REASON_CARRY = "carry"
REASON_VOLUME = "volume"
REASON_LAST_DAY = "last_day"
REASON_EXPIRED = "expired"

SOURCE_TAPE = "лента"
SOURCE_TOTALS = "итоги"
FORTS = "forts"
FORTS_BOARD = "RFUD"
#: Переход по объёму — если следующий контракт больше столько торговых дней подряд
#: (решение владельца 2026-11-17: один день — шум, ролл Eu 07.09).
VOLUME_DAYS = 2
#: Сколько дней вперёд/назад искать торговый день (длинные праздники — до 10 дней).
_SEARCH_DAYS = 14

#: Объёмы бумаг за торговый день: ``(объёмы {secid: контрактов}, источник)`` или ``None``.
VolumeSource = Callable[[date, Sequence[str]], tuple[dict[str, float], str] | None]


@dataclass(frozen=True)
class Decision:
    ticker: str
    reason: str
    basis: str | None = None


# ---------------------------------------------------------------------- #
# Торговые дни
# ---------------------------------------------------------------------- #
def _is_weekday_trading(iss: IssTapeRepository, day: date) -> bool:
    """Будний торговый день FORTS; без календаря — любой будний день."""
    if day.isoweekday() > 5:
        return False
    return is_trading_day(iss, FORTS, day) is not False


def trading_day_of(iss: IssTapeRepository, day: date) -> date:
    """Торговый день, к которому относится календарный день (выходные — к следующему)."""
    probe = day
    for _ in range(_SEARCH_DAYS):
        if _is_weekday_trading(iss, probe):
            return probe
        probe += timedelta(days=1)
    return day


def prior_trading_days(iss: IssTapeRepository, day: date) -> list[date]:
    """P, P−1 — :data:`VOLUME_DAYS` предыдущих будних торговых дней, ближайший первым."""
    found: list[date] = []
    probe: date | None = day
    while probe is not None and len(found) < VOLUME_DAYS:
        probe = previous_trading_day(iss, probe)
        if probe is not None:
            found.append(probe)
    return found


def previous_trading_day(iss: IssTapeRepository, day: date) -> date | None:
    """P — предыдущий будний торговый день до ``day`` (SPEC-0014, шаг 3)."""
    probe = day - timedelta(days=1)
    for _ in range(_SEARCH_DAYS):
        if _is_weekday_trading(iss, probe):
            return probe
        probe -= timedelta(days=1)
    return None


# ---------------------------------------------------------------------- #
# Объёмы
# ---------------------------------------------------------------------- #
class LocalVolumes:
    """Объём за торговый день по локальным данным, без сети (SPEC-0014, «Объём за P»).

    Сначала лента ISS за торговый день (если день не отмечен
    ``ISS_FEED_INCOMPLETE`` и в ней есть сделки этих бумаг), иначе кэш дневных
    итогов ISS. Календарные спреды в ленте и итогах — отдельные ``secid`` и
    в сравнение не попадают.
    """

    def __init__(self, iss: IssTapeRepository) -> None:
        self._iss = iss
        self._tape: dict[str, dict[str, float]] = {}

    def __call__(self, day: date, secids: Sequence[str]) -> tuple[dict[str, float], str] | None:
        key = day.isoformat()
        if self._iss.day_status(FORTS, key, KIND_FEED) != STATUS_INCOMPLETE:
            if key not in self._tape:
                self._tape[key] = self._iss.tape_volumes(FORTS, FORTS_BOARD, key)
            tape = self._tape[key]
            volumes = {s: tape.get(s, 0.0) for s in secids}
            if any(v > 0 for v in volumes.values()):
                return volumes, SOURCE_TAPE
        totals = self._iss.cached_totals(FORTS, key)
        if totals:
            volumes = {s: totals.get(s, (0, 0.0))[1] for s in secids}
            if any(v > 0 for v in volumes.values()):
                return volumes, SOURCE_TOTALS
        return None


# ---------------------------------------------------------------------- #
# Правило
# ---------------------------------------------------------------------- #
def contracts_of(instrument: Instrument, futures: Iterable[FuturesMeta]) -> list[FuturesMeta]:
    """Контракты базового актива по возрастанию последнего дня торгов."""
    own = [m for m in futures if instrument.is_contract_ticker(m.ticker)]
    return sorted(own, key=lambda m: m.active_until)


def _num(value: float) -> str:
    """Объём с пробелами между разрядами: ``523 104``."""
    return f"{value:,.0f}".replace(",", " ")


def _next_after(contracts: Sequence[FuturesMeta], current: FuturesMeta) -> FuturesMeta | None:
    later = [m for m in contracts if m.active_until > current.active_until]
    return later[0] if later else None


def decide(
    instrument: Instrument,
    contracts: Sequence[FuturesMeta],
    trading_day: date,
    previous: str | None,
    prior_days: Sequence[date],
    volumes: VolumeSource,
) -> Decision | None:
    """Контракт на день по правилу SPEC-0014 G-2; ``None`` — у актива нет контракта.

    ``previous`` — тикер последнего решения до этого дня; ``prior_days`` —
    предыдущие будние торговые дни, начиная с ближайшего (P, P−1): переход по
    объёму — только если следующий контракт больше в каждом из них
    (:data:`VOLUME_DAYS`).
    """
    nearest = nearest_contract(instrument, contracts, trading_day)
    if nearest is None:
        return None
    by_ticker = {m.ticker: m for m in contracts}
    current = by_ticker.get(previous) if previous else None
    initial = current is None
    if current is None:
        current = nearest
    # Шаг 1: истёк — ближайший с последним днём торгов не раньше торгового дня.
    if current.active_until < trading_day:
        return Decision(nearest.ticker, REASON_EXPIRED, f"{current.ticker} истёк")
    following = _next_after(contracts, current)
    if following is None:
        return Decision(current.ticker, REASON_INITIAL if initial else REASON_CARRY)
    # Шаг 2: последний день торгов текущего — уже следующий.
    if current.active_until == trading_day:
        return Decision(
            following.ticker, REASON_LAST_DAY, f"последний день торгов {current.ticker}"
        )
    stay = REASON_INITIAL if initial else REASON_CARRY
    # Шаг 3: по объёму за P и P−1 — только с ближайшего на второй ближайший.
    if current.ticker != nearest.ticker or len(prior_days) < VOLUME_DAYS:
        return Decision(current.ticker, stay)
    parts: list[str] = []
    for day in prior_days[:VOLUME_DAYS]:
        found = volumes(day, [current.ticker, following.ticker])
        if found is None:
            logger.warning(
                "ROLL_NO_VOLUME %s %s: нет объёмов за %s — остаётся %s",
                instrument.storage_name,
                trading_day,
                day,
                current.ticker,
            )
            return Decision(current.ticker, stay)
        vols, source = found
        ours, theirs = vols.get(current.ticker, 0.0), vols.get(following.ticker, 0.0)
        parts.append(
            f"{following.ticker} {_num(theirs)} {'>' if theirs > ours else '<='}"
            f" {current.ticker} {_num(ours)} за {day}, {source}"
        )
        if theirs <= ours:
            return Decision(current.ticker, stay, "; ".join(parts))
    return Decision(following.ticker, REASON_VOLUME, "; ".join(parts))


def _log_roll(name: str, day: date, before: str, decision: Decision) -> None:
    detail = {
        REASON_VOLUME: decision.basis or "по объёму",
        REASON_LAST_DAY: f"последний день торгов {before}",
        REASON_EXPIRED: "истёк",
    }.get(decision.reason, decision.reason)
    logger.info("ROLL %s %s: %s -> %s (%s)", name, day, before, decision.ticker, detail)


def ensure_decisions(
    instrument: Instrument,
    repo: MarketDataRepository,
    iss: IssTapeRepository,
    futures: Iterable[FuturesMeta],
    days: Iterable[date],
    volumes: VolumeSource,
    now: datetime | None = None,
) -> dict[date, str]:
    """Принимает решения на дни ``days`` без решения (по порядку); решения дней.

    Предыдущее решение — последнее сохранённое до дня. Сохранённое решение не
    меняется (G-1). Возвращает ``{день: тикер}`` для всех ``days`` с решением.
    """
    contracts = contracts_of(instrument, futures)
    stamp = (now or datetime.now(UTC)).isoformat(timespec="seconds")
    decided = {date.fromisoformat(k): v for k, v in repo.active_contracts().items()}
    result: dict[date, str] = {}
    for day in sorted(set(days)):
        if day in decided:
            result[day] = decided[day]
            continue
        earlier = [d for d in decided if d < day]
        previous = decided[max(earlier)] if earlier else None
        trading = trading_day_of(iss, day)
        decision = decide(
            instrument, contracts, trading, previous, prior_trading_days(iss, trading), volumes
        )
        if decision is None:
            continue
        if repo.save_active_contract(
            day.isoformat(), decision.ticker, decision.reason, decision.basis, stamp
        ):
            if previous is not None and previous != decision.ticker:
                _log_roll(instrument.storage_name, day, previous, decision)
            decided[day] = decision.ticker
        else:  # решение уже приняли параллельно — берём сохранённое (G-1)
            saved = repo.active_contract(day.isoformat())
            decided[day] = str(saved["ticker"]) if saved else decision.ticker
        result[day] = decided[day]
    return result


# ---------------------------------------------------------------------- #
# Обслуживание: решения на все дни и перевод истории (SPEC-0014 G-4)
# ---------------------------------------------------------------------- #
#: Флаг ``meta``: история инструмента переведена на новое правило.
#: ``_v2``: первый вариант смотрел только запись сверки и пропускал дни, данные
#: которых перекачаны из архива по SPEC-0013 (BR 02.11, NG 28.10) — повторить.
MIGRATED_META_KEY = "spec0014_roll_migrated_v2"
#: Прежняя запись контракта дня сверкой (до SPEC-0014).
LEGACY_CONTRACT_PREFIX = "contract:"


def migrate_history(
    instrument: Instrument,
    repo: MarketDataRepository,
    futures: Mapping[str, FuturesMeta],
    decisions: Mapping[date, str],
    today: date,
) -> list[date]:
    """Отмечает ``roll_rule`` дни с данными, где контракт по решению другой (G-4).

    Откуда взяты прежние данные дня, не записано: стрим (контракт — запись
    сверки ``meta['contract:D']``) или архив (ближайший по последнему дню
    торгов, SPEC-0013). День отмечается, если решение расходится хоть с одним
    из них; лишняя перекачка безвредна. Выполняется один раз на инструмент.
    """
    if repo.get_meta(MIGRATED_META_KEY):
        return []
    changed: list[date] = []
    for day_str in sorted(repo.available_days()):
        day = date.fromisoformat(day_str)
        if day >= today or day not in decisions:
            continue
        nearest = nearest_contract(instrument, futures.values(), day)
        legacy = {
            t
            for t in (
                repo.get_meta(f"{LEGACY_CONTRACT_PREFIX}{day_str}"),
                nearest.ticker if nearest is not None else None,
            )
            if t
        }
        if legacy - {decisions[day]}:
            logger.warning(
                "ROLL_RULE %s %s: %s -> %s, отмечен для перекачки",
                instrument.storage_name,
                day,
                "/".join(sorted(legacy)),
                decisions[day],
            )
            changed.append(day)
    repo.mark_days_for_refresh([d.isoformat() for d in changed], REFRESH_ROLL_RULE, override=True)
    repo.set_meta(MIGRATED_META_KEY, datetime.now(UTC).isoformat(timespec="seconds"))
    return changed


def update_instrument(
    instrument: Instrument,
    repo: MarketDataRepository,
    iss: IssTapeRepository,
    futures: Mapping[str, FuturesMeta],
    today: date,
    volumes: VolumeSource,
) -> dict[date, str]:
    """Ночное обслуживание: решения с первого сохранённого дня по сегодня, затем перевод."""
    stored = [date.fromisoformat(d) for d in repo.available_days()]
    start = min([*stored, today])
    days = [start + timedelta(days=k) for k in range((today - start).days + 1)]
    decisions = ensure_decisions(instrument, repo, iss, futures.values(), days, volumes)
    migrate_history(instrument, repo, futures, decisions, today)
    return decisions
