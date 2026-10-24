"""Прогресс и наблюдение: снимки, EWMA rate/latency, expected/ETA, Observer (§4.8, §6).

Снимок (`PipelineView`/`StageView`) дешёвый и синхронный; `expected` считается по
таблице §6 (закрыто → found; подсказка → max(found,n); оценка по писателям; цикл →
None). EWMA скорости и латентности — на `Clock`, поэтому ETA детерминирован в тестах.
"""

import logging
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from enum import Enum, auto
from typing import Protocol, runtime_checkable

from staged_pipeline._accounting import Ledger
from staged_pipeline._clock import Clock
from staged_pipeline._graph import StageSpec, cyclic_stage_names

__all__ = [
    "EwmaTracker",
    "LoggingObserver",
    "Observer",
    "PipelineReport",
    "PipelineView",
    "StageState",
    "StageView",
    "build_pipeline_view",
]

_SAMPLE_CAP = 20
_SAMPLE_FRACTION = 0.05
_EWMA_ALPHA = 0.3


class StageState(Enum):
    """Состояние стадии в снимке (architecture.md §4.8)."""

    OPEN = auto()
    CLOSING = auto()
    CLOSED = auto()
    CANCELLED = auto()
    FAILED = auto()


class EwmaTracker:
    """EWMA скорости завершений и латентности хендлера на часах `Clock`."""

    def __init__(self, clock: Clock) -> None:
        """Создать трекер."""
        self._clock: Clock = clock
        self._rate: float = 0.0
        self._latency: float = 0.0
        self._last: float | None = None
        self._samples: int = 0

    def record(self, latency: float) -> None:
        """Зафиксировать одно завершение с длительностью `latency`."""
        now = self._clock.now()
        self._latency = (
            latency
            if self._samples == 0
            else _EWMA_ALPHA * latency + (1 - _EWMA_ALPHA) * self._latency
        )
        if self._last is not None:
            dt = now - self._last
            inst = 1.0 / dt if dt > 0 else 0.0
            self._rate = (
                inst if self._rate == 0.0 else _EWMA_ALPHA * inst + (1 - _EWMA_ALPHA) * self._rate
            )
        self._last = now
        self._samples += 1

    @property
    def rate(self) -> float:
        """EWMA завершений в секунду."""
        return self._rate

    @property
    def latency(self) -> float:
        """EWMA длительности хендлера, секунды."""
        return self._latency


@dataclass(frozen=True, slots=True)
class StageView:
    """Снимок одной стадии (поля как у Tallyho `Progress` плюс локальные)."""

    state: StageState
    found: int
    queued: int
    in_flight: int
    deferred: int
    ok: int
    skip: int
    error: int
    cancelled: int
    duplicates: int
    labels: Mapping[str, int]
    metrics: Mapping[str, int]
    expected: int | None
    expected_is_estimate: bool
    estimate_basis: str
    final: bool
    rate: float
    latency: float
    eta: float | None
    emitted: Mapping[str, int]

    @property
    def done(self) -> int:
        """`ok + skip + error + cancelled`."""
        return self.ok + self.skip + self.error + self.cancelled


@dataclass(frozen=True, slots=True)
class PipelineView:
    """Снимок всего пайплайна."""

    name: str
    stages: Mapping[str, StageView]
    eta: float | None

    def __getitem__(self, stage: str) -> StageView:
        """Снимок стадии по имени."""
        return self.stages[stage]


@dataclass(frozen=True, slots=True)
class PipelineReport:
    """Итог прогона: последний снимок + длительность + причина остановки (§4.8)."""

    view: PipelineView
    duration: float
    stopped: str | None

    @property
    def stages(self) -> Mapping[str, StageView]:
        """Снимки стадий."""
        return self.view.stages

    @property
    def final(self) -> bool:
        """Все стадии закрыты (числа окончательные)."""
        return all(s.final for s in self.view.stages.values())


@runtime_checkable
class Observer(Protocol):
    """Наблюдатель событий пайплайна (все методы опциональны)."""

    def stage_closed(self, stage: str) -> None:
        """Стадия закрылась."""
        ...

    def item_finished(
        self, stage: str, key: object, outcome: str, exc: BaseException | None
    ) -> None:
        """Элемент завершён с исходом `outcome`."""
        ...

    def breaker_tripped(self, stage: str, reason: str) -> None:
        """Сработал предохранитель стадии."""
        ...

    def queued_warning(self, stage: str, queued: int) -> None:
        """Очередь стадии превысила `warn_queued`."""
        ...


class LoggingObserver:
    """Наблюдатель по умолчанию: пишет события в логгер."""

    def __init__(self, log: logging.Logger) -> None:
        """Создать наблюдатель поверх логгера."""
        self._log: logging.Logger = log

    def stage_closed(self, stage: str) -> None:
        """Залогировать закрытие стадии (DEBUG)."""
        self._log.debug("стадия %s закрыта", stage)

    def item_finished(
        self, stage: str, key: object, outcome: str, exc: BaseException | None
    ) -> None:
        """Ничего (исходы skip/error логирует воркер отдельно)."""
        del stage, key, outcome, exc

    def breaker_tripped(self, stage: str, reason: str) -> None:
        """Залогировать срабатывание предохранителя (WARNING)."""
        self._log.warning("стадия %s: предохранитель сработал: %s", stage, reason)

    def queued_warning(self, stage: str, queued: int) -> None:
        """Залогировать переполнение очереди (WARNING)."""
        self._log.warning("стадия %s: очередь выросла до %d", stage, queued)


def _incoming(specs: Sequence[StageSpec]) -> dict[str, list[str]]:
    """Построить карту «стадия → имена писателей (стадий с ребром в неё)»."""
    incoming: dict[str, list[str]] = {spec.name: [] for spec in specs}
    for spec in specs:
        for target in spec.targets:
            incoming[target.name].append(spec.name)
    return incoming


def _state(name: str, ledger: Ledger, *, stopped: str | None, failed: bool) -> StageState:
    """Определить состояние стадии для снимка."""
    if failed:
        return StageState.FAILED
    if ledger.is_closed(name):
        return StageState.CLOSED
    if stopped is not None:
        return StageState.CANCELLED
    if ledger.component_writers(name) == 0:
        return StageState.CLOSING
    return StageState.OPEN


def _expected(  # noqa: PLR0913, PLR0917  # вычисление expected требует всего контекста §6
    name: str,
    ledger: Ledger,
    incoming: Mapping[str, list[str]],
    expects: Mapping[str, int],
    cyclic: set[str],
    memo: dict[str, tuple[int | None, bool, str]],
) -> tuple[int | None, bool, str]:
    """Вычислить (expected, is_estimate, basis) стадии по таблице §6."""
    if name in memo:
        return memo[name]
    counts = ledger.counts(name)
    result: tuple[int | None, bool, str]
    # Строка 1: вход закрыт (writers == 0) → точное значение found.
    if ledger.component_writers(name) == 0:
        result = (counts.found, False, "closed")
    else:
        result = _expected_open(name, ledger, incoming, expects, cyclic, memo, counts.found)
    memo[name] = result
    return result


def _expected_open(  # noqa: PLR0913, PLR0917  # вычисление оценки требует всего контекста §6
    name: str,
    ledger: Ledger,
    incoming: Mapping[str, list[str]],
    expects: Mapping[str, int],
    cyclic: set[str],
    memo: dict[str, tuple[int | None, bool, str]],
    found: int,
) -> tuple[int | None, bool, str]:
    """Оценка expected для открытой стадии (строки 2–5 таблицы §6)."""
    # Строка 2: задана подсказка expect(n).
    hint = expects.get(name, 0)
    if hint > 0:
        return (max(found, hint), True, "hint")
    # Строка 4: стадия в цикле и не закрыта → None (только фронтир).
    if name in cyclic:
        return (None, False, "cycle")
    # Строка 3: у всех писателей есть expected и выборка достаточна.
    writers = incoming.get(name, [])
    if not writers:
        return (None, False, "unknown")
    memo[name] = (None, False, "pending")  # защита от рекурсии по циклу
    projected = 0.0
    for writer in writers:
        exp_w, _, _ = _expected(writer, ledger, incoming, expects, cyclic, memo)
        if exp_w is None:
            return (None, False, "unknown")
        done_w = ledger.counts(writer).done
        if done_w < min(_SAMPLE_CAP, _SAMPLE_FRACTION * exp_w):
            return (None, False, "unknown")
        emitted_to = ledger.counts(writer).emitted.get(name, 0)
        projected += (emitted_to / done_w) * exp_w if done_w else 0.0
    return (max(found, round(projected)), True, "estimate")


def build_pipeline_view(  # noqa: PLR0913  # снимок требует всех источников состояния
    name: str,
    specs: Sequence[StageSpec],
    ledger: Ledger,
    trackers: Mapping[str, EwmaTracker],
    expects: Mapping[str, int],
    *,
    stopped: str | None,
    failed: bool,
) -> PipelineView:
    """Собрать снимок пайплайна (дешёвый, синхронный)."""
    incoming = _incoming(specs)
    cyclic = cyclic_stage_names(specs)
    memo: dict[str, tuple[int | None, bool, str]] = {}
    stages: dict[str, StageView] = {}
    etas: list[float | None] = []
    for spec in specs:
        stage_name = spec.name
        counts = ledger.counts(stage_name)
        expected, is_estimate, basis = _expected(
            stage_name, ledger, incoming, expects, cyclic, memo
        )
        tracker = trackers[stage_name]
        done = counts.ok + counts.skip + counts.error + counts.cancelled
        eta = (
            (expected - done) / tracker.rate if expected is not None and tracker.rate > 0 else None
        )
        etas.append(eta)
        state = _state(stage_name, ledger, stopped=stopped, failed=failed)
        stages[stage_name] = StageView(
            state=state,
            found=counts.found,
            queued=counts.queued,
            in_flight=counts.in_flight,
            deferred=counts.deferred,
            ok=counts.ok,
            skip=counts.skip,
            error=counts.error,
            cancelled=counts.cancelled,
            duplicates=counts.duplicates,
            labels=dict(counts.labels),
            metrics=dict(counts.metrics),
            expected=expected,
            expected_is_estimate=is_estimate,
            estimate_basis=basis,
            final=ledger.is_closed(stage_name),
            rate=tracker.rate,
            latency=tracker.latency,
            eta=eta,
            emitted=dict(counts.emitted),
        )
    pipeline_eta = (
        max(e for e in etas if e is not None) if etas and all(e is not None for e in etas) else None
    )
    return PipelineView(name=name, stages=stages, eta=pipeline_eta)
