"""Рантайм стадии: контекст, воркер-цикл, нормализация выхода, маршрутизация (§4.3, §7.4).

`Stage` — то, что держит пользователь (обёртка над `StageSpec` из `_graph`): inbox,
seed-элементы, рёбра. Воркер берёт элемент из inbox, вызывает хендлер, раскладывает
выход по рёбрам (emit вне permit-а — §7.5) и фиксирует исход в `Ledger`. В T2.3
политик нет: любое исключение хендлера — сбой пайплайна (D-001).
"""

import asyncio
import logging
from collections.abc import Hashable, Iterable, Sequence
from contextlib import AsyncExitStack, aclosing
from types import TracebackType
from typing import TYPE_CHECKING, Protocol, Self, cast, runtime_checkable

from staged_pipeline._accounting import Event, Ledger
from staged_pipeline._clock import Clock
from staged_pipeline._errors import (
    Error,
    RoutingError,
    StageAborted,
    StageClosedError,
    validate_label,
)
from staged_pipeline._graph import Fallback, HandlerKind, KeyFn, StageSpec
from staged_pipeline._inbox import Envelope, Inbox
from staged_pipeline._limit import Limiter
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

__all__ = ["Feeder", "SeenStore", "SetSeenStore", "Stage", "StageContext"]


@runtime_checkable
class SeenStore(Protocol):
    """Хранилище принятых ключей для дедупликации (architecture.md §4.6)."""

    def add(self, key: Hashable) -> bool:
        """Добавить ключ; вернуть True, если он новый (ещё не встречался)."""
        ...

    def __contains__(self, key: object) -> bool:
        """Был ли ключ уже принят."""
        ...


class SetSeenStore:
    """Реализация `SeenStore` поверх `set` (v1)."""

    def __init__(self) -> None:
        """Создать пустое хранилище."""
        self._seen: set[Hashable] = set()

    def add(self, key: Hashable) -> bool:
        """Добавить ключ; True — если он был новым."""
        if key in self._seen:
            return False
        self._seen.add(key)
        return True

    def __contains__(self, key: object) -> bool:
        """Был ли ключ принят."""
        return key in self._seen


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

    def __init__(self, spec: StageSpec, *, seen: SeenStore | None = None) -> None:
        """Обернуть спецификацию стадии."""
        self.spec: StageSpec = spec
        self.seed_items: list[object] = []
        self.has_auto_seed: bool = False
        self.feeders: list[Feeder] = []
        # Дедупликация и resume (architecture.md §4.6).
        self.seen: SeenStore = seen if seen is not None else SetSeenStore()
        self.resumed: set[Hashable] = set()
        self.resume_label: str = "resumed"
        # Последнее исходное исключение (для заметки при срабатывании abort_if).
        self.last_exc: BaseException | None = None
        # inbox навешивается при старте run().
        self.inbox: Inbox[object] | None = None

    @property
    def key(self) -> KeyFn | None:
        """Функция ключа дедупликации (или None)."""
        return self.spec.key

    def to(self, *targets: "Stage", fallback: str = "error") -> None:
        """Объявить рёбра к стадиям-получателям (делегирует `StageSpec.to`)."""
        fb: Fallback = "drop" if fallback == "drop" else "error"
        self.spec.to(*(t.spec for t in targets), fallback=fb)

    def seed(self, items: Iterable[object]) -> None:
        """Засеять вход элементами (вход закроется при старте `run()`)."""
        self.spec.seeded = True
        self.has_auto_seed = True
        self.seed_items.extend(items)

    def resume(self, keys: Iterable[Hashable], *, label: str = "resumed") -> None:
        """Пометить ключи уже сделанного: при встрече → `found` + `skip(label)` (§4.6)."""
        self.resume_label = validate_label(label)
        self.resumed.update(keys)

    def seed_open(self) -> "Feeder":
        """Открыть вход-Feeder: вход не закроется, пока `Feeder.close()` не вызван."""
        self.spec.seeded = True
        feeder = Feeder(self)
        self.feeders.append(feeder)
        return feeder


class Feeder:
    """Открытый вход стадии: `put()` во время прогона, `close()` закрывает (§4.6)."""

    def __init__(self, stage: Stage) -> None:
        """Создать Feeder, привязанный к стадии (активируется при старте run)."""
        self._stage: Stage = stage
        self._ledger: Ledger | None = None
        self._stages: dict[str, Stage] | None = None
        self._closed: bool = False

    def bind(self, ledger: Ledger, stages: dict[str, Stage]) -> None:
        """Привязать Feeder к рантайму (вызывается из `Pipeline.run`)."""
        self._ledger = ledger
        self._stages = stages

    async def put(self, item: object) -> None:
        """Предложить элемент стадии (проходит dedup/resume).

        Raises:
            RuntimeError: пайплайн ещё не запущен.
            StageClosedError: Feeder уже закрыт.
        """
        if self._ledger is None or self._stages is None:
            raise RuntimeError("Feeder.put до запуска пайплайна")
        if self._closed:
            raise StageClosedError("Feeder уже закрыт")
        await offer(self._stage, item, ledger=self._ledger, stages=self._stages)

    def close(self) -> None:
        """Закрыть вход: снять writer компоненты. Идемпотентно."""
        if self._closed or self._ledger is None:
            return
        self._closed = True
        self._ledger.seal_seed(self._stage.spec.name)

    async def __aenter__(self) -> "Self":
        """Войти в контекст (Feeder уже открыт)."""
        return self

    async def __aexit__(self, *exc: object) -> None:
        """Выйти из контекста — закрыть вход."""
        self.close()


def _normalize(result: object) -> list[object]:
    """Привести выход корутины к списку элементов (architecture.md §4.3)."""
    if result is None:
        return []
    if isinstance(result, (list, tuple)):
        return list(cast("Sequence[object]", result))
    return [result]


def _route(stage: Stage, out: object, ledger: Ledger) -> StageSpec | None:
    """Выбрать спецификацию стадии-получателя по типу выхода (architecture.md §4.2).

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


async def offer(
    stage: Stage,
    item: object,
    *,
    ledger: Ledger,
    stages: dict[str, Stage],
    nowait: bool = False,
) -> None:
    """Предложить элемент стадии: dedup/resume, затем постановка в очередь (§4.6).

    Один путь приёма для seed, emit и Feeder: ключ помечается принятым при приёме
    (ретрай его не проходит повторно, т.к. использует `defer`). resumed-ключ даёт
    `found`+`skip`; повтор принятого ключа — `duplicate`. `nowait` — для засева до
    старта воркеров (не ждёт место в bounded-очереди).
    """
    name = stage.spec.name
    inbox = stages[name].inbox
    if inbox is None:  # pragma: no cover - offer зовётся только во время run
        raise RuntimeError("стадия не инициализирована")
    if inbox.closed:
        # Стадия уже закрыта (cancel/drain или завершена) — тихо отбрасываем.
        ledger.incr(name, "dropped_on_stop")
        return
    key = stage.key(item) if stage.key is not None else None
    if key is not None and isinstance(key, Hashable):
        if key in stage.resumed:
            stage.resumed.discard(key)
            stage.seen.add(key)
            ledger.transition(name, Event.RESUME, label=stage.resume_label)
            return
        if not stage.seen.add(key):
            ledger.transition(name, Event.DUPLICATE)
            return
    ledger.transition(name, Event.ACCEPT)
    if nowait:
        inbox.put_nowait(Envelope(item))
    else:
        await inbox.put(Envelope(item))


async def _emit(stage: Stage, out: object, stages: dict[str, Stage], ledger: Ledger) -> None:
    """Отправить один выходной элемент получателю (ACCEPT до выхода родителя, §7.2)."""
    target = _route(stage, out, ledger)
    if target is None:
        return
    ledger.emit(stage.spec.name, target.name)
    await offer(stages[target.name], out, ledger=ledger, stages=stages)


class _Permits:
    """Совокупный async-permit всех лимитеров стадии (нулевой при отсутствии)."""

    def __init__(self, limits: tuple[Limiter, ...]) -> None:
        """Запомнить лимитеры."""
        self._limits: tuple[Limiter, ...] = limits
        self._stack: AsyncExitStack = AsyncExitStack()

    async def __aenter__(self) -> None:
        """Занять все лимитеры по порядку."""
        await self._stack.__aenter__()
        for limiter in self._limits:
            await self._stack.enter_async_context(limiter.acquire())

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        """Освободить все лимитеры."""
        await self._stack.__aexit__(exc_type, exc, tb)


async def _call(stage: Stage, env: Envelope[object], ctx: StageContext | None) -> object:
    """Вызвать корутину/sync-хендлер под permit-ом лимитеров; вернуть результат."""
    spec = stage.spec
    async with _Permits(spec.limits):
        if spec.info.kind is HandlerKind.SYNC:
            if ctx is not None:
                return await asyncio.to_thread(spec.handler, env.item, ctx)
            return await asyncio.to_thread(spec.handler, env.item)
        raw = spec.handler(env.item, ctx) if ctx is not None else spec.handler(env.item)
        return await cast("Awaitable[object]", raw)


async def _process(
    stage: Stage,
    env: Envelope[object],
    *,
    clock: Clock,
    stages: dict[str, Stage],
    ledger: Ledger,
) -> str:
    """Вызвать хендлер и разложить выход по рёбрам; permit — вокруг вызова, emit — вне (§7.5)."""
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
            while True:
                async with _Permits(spec.limits):
                    try:
                        out = await stream.__anext__()
                    except StopAsyncIteration:
                        break
                await _emit(stage, out, stages, ledger)  # emit вне permit-а
    else:
        result = await _call(stage, env, ctx)
        for out in _normalize(result):
            await _emit(stage, out, stages, ledger)
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


async def run_worker(  # noqa: PLR0913  # воркеру нужны карта стадий, ledger, часы, лог, супервизор
    stage: Stage,
    *,
    clock: Clock,
    stages: dict[str, Stage],
    ledger: Ledger,
    log: logging.Logger,
    supervisor: Supervisor,
) -> None:
    """Цикл воркера с политиками (§4.4, §4.5): ретрай/skip/error/abort и логирование."""
    name = stage.spec.name
    inbox = stage.inbox
    if inbox is None:  # pragma: no cover - воркер запускается только с inbox
        raise RuntimeError("стадия не инициализирована")
    try:
        while True:
            env = await inbox.get()
            if env is None:
                _check_breakers(stage, ledger, closing=True)
                return
            ledger.transition(name, Event.START)
            await _handle(stage, env, clock=clock, stages=stages, ledger=ledger, log=log)
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
    stages: dict[str, Stage],
    ledger: Ledger,
    log: logging.Logger,
) -> None:
    """Обработать один элемент: вызвать хендлер и классифицировать исключение (§4.4)."""
    name = stage.spec.name
    inbox = stage.inbox
    if inbox is None:  # pragma: no cover - воркер запускается только с inbox
        raise RuntimeError("стадия не инициализирована")
    try:
        label = await _process(stage, env, clock=clock, stages=stages, ledger=ledger)
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
                inbox.defer(Envelope(env.item, env.attempt + 1), clock.now() + delay)
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
