"""Рантайм стадии: контекст, воркер-цикл, нормализация выхода, маршрутизация (§4.3, §7.4).

`Stage` — то, что держит пользователь (обёртка над `StageSpec` из `_graph`): inbox,
seed-элементы, рёбра. Воркер берёт элемент из inbox, вызывает хендлер, раскладывает
выход по рёбрам (emit вне permit-а — §7.5) и фиксирует исход в `Ledger`. В T2.3
политик нет: любое исключение хендлера — сбой пайплайна (D-001).
"""

import logging
from collections.abc import Iterable, Sequence
from contextlib import aclosing
from typing import TYPE_CHECKING, cast

from staged_pipeline._accounting import Event, Ledger
from staged_pipeline._clock import Clock
from staged_pipeline._errors import Error, RoutingError, StageAborted, validate_label
from staged_pipeline._graph import Fallback, StageSpec
from staged_pipeline._inbox import Envelope, Inbox
from staged_pipeline._policy import (
    Breaker,
    BreakerStats,
    Deferral,
    ErrorOutcome,
    Failure,
    SkipOutcome,
    classify,
)
from staged_pipeline._supervisor import Supervisor

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Awaitable

__all__ = ["Stage", "StageContext"]


class StageContext:
    """Контекст, передаваемый хендлеру вторым параметром (architecture.md §4.3)."""

    def __init__(self, *, stage: str, attempt: int, clock: Clock, ledger: Ledger) -> None:
        """Создать контекст для одного вызова хендлера.

        Args:
            stage: имя стадии.
            attempt: номер попытки (1-based).
            clock: часы пайплайна.
            ledger: реестр счётчиков (для `incr`).
        """
        self.stage: str = stage
        self.attempt: int = attempt
        self.clock: Clock = clock
        self._ledger: Ledger = ledger
        self._label: str = "ok"

    @property
    def chosen_label(self) -> str:
        """Метка, выбранная хендлером для успешного исхода."""
        return self._label

    def label(self, label: str) -> None:
        """Задать метку успешного исхода (по умолчанию `ok`)."""
        self._label = validate_label(label)

    def incr(self, name: str, n: int = 1) -> None:
        """Увеличить доменную метрику стадии."""
        self._ledger.incr(self.stage, name, n)


class Stage:
    """Стадия пайплайна: обёртка над `StageSpec` с рантайм-состоянием.

    Пользователь получает `Stage` из `Pipeline.stage(...)`, объявляет рёбра
    `.to(...)` и засев `.seed(...)`. Типы `In`/`Out` выводятся из аннотаций
    хендлера рантаймом (статический вывод отложен, D-012).
    """

    def __init__(self, spec: StageSpec) -> None:
        """Обернуть спецификацию стадии."""
        self.spec: StageSpec = spec
        self.seed_items: list[object] = []
        # Последнее исходное исключение (для заметки при срабатывании abort_if).
        self.last_exc: BaseException | None = None

    def to(self, *targets: "Stage", fallback: str = "error") -> None:
        """Объявить рёбра к стадиям-получателям (делегирует `StageSpec.to`)."""
        fb: Fallback = "drop" if fallback == "drop" else "error"
        self.spec.to(*(t.spec for t in targets), fallback=fb)

    def seed(self, items: Iterable[object]) -> None:
        """Засеять вход элементами (вход закроется при старте `run()`)."""
        self.spec.seeded = True
        self.seed_items.extend(items)


def _normalize(result: object) -> list[object]:
    """Привести выход корутины к списку элементов (architecture.md §4.3)."""
    if result is None:
        return []
    if isinstance(result, (list, tuple)):
        return list(cast("Sequence[object]", result))
    return [result]


def _route(stage: Stage, out: object, ledger: Ledger) -> StageSpec | None:
    """Выбрать стадию-получателя по типу выхода (architecture.md §4.2).

    Returns:
        Спецификация получателя или None (терминальная стадия / `drop`).

    Raises:
        RoutingError: выход не подошёл ни к одной стадии при `fallback="error"`.
    """
    targets = stage.spec.targets
    if not targets:
        return None
    if len(targets) == 1:
        return targets[0]
    matches = [t for t in targets if any(isinstance(out, it) for it in t.info.in_types)]
    if len(matches) == 1:
        return matches[0]
    if not matches:
        if stage.spec.fallback == "drop":
            ledger.incr(stage.spec.name, "unrouted")
            return None
        raise RoutingError(
            f"стадия {stage.spec.name!r}: выход типа {type(out).__name__} "
            + "не подошёл ни к одной стадии-получателю"
        )
    names = ", ".join(t.name for t in matches)
    raise RoutingError(
        f"стадия {stage.spec.name!r}: выход типа {type(out).__name__} "
        + f"подошёл к нескольким стадиям ({names})"
    )


async def _emit(
    stage: Stage, out: object, inboxes: dict[str, Inbox[object]], ledger: Ledger
) -> None:
    """Отправить один выходной элемент получателю (ACCEPT до выхода родителя, §7.2)."""
    target = _route(stage, out, ledger)
    if target is None:
        return
    ledger.emit(stage.spec.name, target.name)
    ledger.transition(target.name, Event.ACCEPT)
    await inboxes[target.name].put(Envelope(out))


async def _process(
    stage: Stage,
    env: Envelope[object],
    *,
    clock: Clock,
    inboxes: dict[str, Inbox[object]],
    ledger: Ledger,
) -> str:
    """Вызвать хендлер и разложить его выход по рёбрам; вернуть метку исхода ok."""
    spec = stage.spec
    info = spec.info
    ctx = (
        StageContext(stage=spec.name, attempt=env.attempt, clock=clock, ledger=ledger)
        if info.wants_ctx
        else None
    )
    if info.streaming:
        raw = spec.handler(env.item, ctx) if ctx is not None else spec.handler(env.item)
        gen = cast("AsyncGenerator[object]", raw)
        async with aclosing(gen) as stream:
            async for out in stream:
                await _emit(stage, out, inboxes, ledger)
    else:
        raw = spec.handler(env.item, ctx) if ctx is not None else spec.handler(env.item)
        result = await cast("Awaitable[object]", raw)
        for out in _normalize(result):
            await _emit(stage, out, inboxes, ledger)
    return ctx.chosen_label if ctx is not None else "ok"


def _log_outcome(
    log: logging.Logger, stage: Stage, env: Envelope[object], exc: Exception, kind: str
) -> None:
    """Залогировать обработанный исход: WARNING строкой, DEBUG — трейсбек."""
    level = stage.spec.log_levels.get(kind, logging.WARNING)
    key = stage.spec.key(env.item) if stage.spec.key is not None else None
    log.log(level, "%s %s: %s: %s", stage.spec.name, key, type(exc).__name__, exc)
    if log.isEnabledFor(logging.DEBUG):
        log.debug("трейсбек исхода %s стадии %s", kind, stage.spec.name, exc_info=exc)


def _breaker_stats(stage: Stage, ledger: Ledger, *, closing: bool) -> BreakerStats:
    """Собрать снимок счётчиков стадии для проверки предохранителей."""
    counts = ledger.counts(stage.spec.name)
    return BreakerStats(ok=counts.ok, error=counts.error, labels=counts.labels, closing=closing)


def _check_breakers(stage: Stage, ledger: Ledger, *, closing: bool) -> None:
    """Проверить предохранители; при срабатывании бросить исходное исключение/StageAborted."""
    if not stage.spec.abort_if:
        return
    stats = _breaker_stats(stage, ledger, closing=closing)
    for breaker in stage.spec.abort_if:
        if breaker.tripped(stats):
            raise _aborted(stage, breaker, stats)


def _aborted(stage: Stage, breaker: Breaker, stats: BreakerStats) -> BaseException:
    """Построить исключение срабатывания предохранителя (§4.5)."""
    note = f"aborted by {breaker.reason(stats)} at {stage.spec.name}"
    exc = stage.last_exc
    if exc is not None:
        exc.add_note(note)
        return exc
    return StageAborted(note)


async def run_worker(  # noqa: PLR0913  # воркеру нужны inbox, карта inbox-ов, ledger, лог, супервизор
    stage: Stage,
    *,
    clock: Clock,
    inbox: Inbox[object],
    inboxes: dict[str, Inbox[object]],
    ledger: Ledger,
    log: logging.Logger,
    supervisor: Supervisor,
) -> None:
    """Цикл воркера с политиками (§4.4, §4.5): ретрай/skip/error/abort и логирование."""
    name = stage.spec.name
    try:
        while True:
            env = await inbox.get()
            if env is None:
                _check_breakers(stage, ledger, closing=True)
                return
            ledger.transition(name, Event.START)
            await _handle(stage, env, clock=clock, inboxes=inboxes, ledger=ledger, log=log)
            _check_breakers(stage, ledger, closing=False)
    except Exception as exc:  # сбой/abort: зафиксировать первым и погасить остальных
        exc.add_note(f"stage={name}")
        supervisor.record(exc)
        raise


async def _handle(  # noqa: PLR0913  # обработчик элемента: нужны все рантайм-зависимости
    stage: Stage,
    env: Envelope[object],
    *,
    clock: Clock,
    inboxes: dict[str, Inbox[object]],
    ledger: Ledger,
    log: logging.Logger,
) -> None:
    """Обработать один элемент: вызвать хендлер и классифицировать исключение (§4.4)."""
    name = stage.spec.name
    try:
        label = await _process(stage, env, clock=clock, inboxes=inboxes, ledger=ledger)
    except Exception as exc:
        decision = classify(
            exc,
            retry=stage.spec.retry,
            skip_on=stage.spec.skip_on,
            error_on=stage.spec.error_on,
            attempt=env.attempt,
        )
        match decision:
            case Deferral(delay=delay):
                ledger.transition(name, Event.DEFER)
                inboxes[name].defer(Envelope(env.item, env.attempt + 1), clock.now() + delay)
            case SkipOutcome(label=skip_label):
                _log_outcome(log, stage, env, exc, "skip")
                ledger.transition(name, Event.SKIP, label=skip_label)
            case ErrorOutcome(label=error_label):
                # Прямой raise Error — не «исходное» исключение (§4.5): abort даст
                # StageAborted. Доменное исключение из error_on — исходное.
                if not isinstance(exc, Error):
                    stage.last_exc = exc
                _log_outcome(log, stage, env, exc, "error")
                ledger.transition(name, Event.ERROR, label=error_label)
            case Failure():
                stage.last_exc = exc
                exc.add_note(f"attempt={env.attempt}")
                raise
        return
    ledger.transition(name, Event.OK, label=label)
