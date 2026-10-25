"""Сборка и запуск пайплайна: `Pipeline`, `Pipeline.stage`, `seed`, `run` (§4.1, §7.3).

`Pipeline` — граф + супервизор + часы, одноразовый (один `run()`). `run()` проверяет
топологию до старта, засевает входы, запускает по пулу воркеров на стадию в одной
TaskGroup и ждёт естественного завершения (закрытие всех компонент по §7.2).
"""

import asyncio
import logging
from collections.abc import AsyncIterator, Callable, Coroutine, Mapping, Sequence

from staged_pipeline._accounting import Event, Ledger, StageCounts
from staged_pipeline._clock import Clock, MonotonicClock
from staged_pipeline._errors import ConfigurationError
from staged_pipeline._graph import KeyFn, StageSpec, validate_topology
from staged_pipeline._inbox import Envelope, Inbox
from staged_pipeline._limit import Limiter
from staged_pipeline._policy import Breaker, ExcMap, Retry
from staged_pipeline._progress import (
    EwmaTracker,
    LoggingObserver,
    Observer,
    PipelineReport,
    PipelineView,
    StageState,
    build_pipeline_view,
    safe_observer,
)
from staged_pipeline._stage import Stage, offer, run_worker
from staged_pipeline._supervisor import Supervisor

__all__ = ["Pipeline"]

_log = logging.getLogger("staged_pipeline")


def _normalize_breakers(abort_if: Breaker | Sequence[Breaker] | None) -> tuple[Breaker, ...]:
    """Привести `abort_if` к кортежу предохранителей."""
    if abort_if is None:
        return ()
    if isinstance(abort_if, Breaker):
        return (abort_if,)
    return tuple(abort_if)


class Pipeline:
    """Граф стадий, супервизор и часы. Одноразовый: ровно один `run()`."""

    def __init__(
        self,
        name: str,
        *,
        clock: Clock | None = None,
        log: logging.Logger | None = None,
        observer: Observer | None = None,
    ) -> None:
        """Создать пайплайн.

        Args:
            name: имя (для логов).
            clock: часы; по умолчанию `MonotonicClock`.
            log: логгер; по умолчанию `staged_pipeline`.
            observer: наблюдатель событий; по умолчанию логирующий.
        """
        self.name: str = name
        self._clock: Clock = clock or MonotonicClock()
        self._log: logging.Logger = log or _log
        self._observer: Observer = safe_observer(observer or LoggingObserver(self._log), self._log)
        self._stages: dict[str, Stage] = {}
        self._ran: bool = False
        self._ledger: Ledger | None = None
        self._supervisor: Supervisor | None = None
        self._trackers: dict[str, EwmaTracker] = {}
        self.stopped: str | None = None

    def stage(  # noqa: PLR0913  # публичный конфигуратор стадии: много опций по §4.1
        self,
        name: str,
        handler: Callable[..., object],
        *,
        workers: int = 1,
        buffer: int | None = None,
        key: KeyFn | None = None,
        retry: Retry | None = None,
        skip_on: ExcMap | None = None,
        error_on: ExcMap | None = None,
        abort_if: Breaker | Sequence[Breaker] | None = None,
        limits: Sequence[Limiter] | None = None,
        log_levels: Mapping[str, int] | None = None,
        executor: str | None = None,
        warn_queued: int | None = None,
    ) -> Stage:
        """Создать и зарегистрировать стадию.

        Returns:
            Рантайм-`Stage` для объявления рёбер `.to(...)` и засева `.seed(...)`.

        Raises:
            ConfigurationError: имя стадии не уникально.
        """
        if name in self._stages:
            raise ConfigurationError(f"стадия {name!r} уже объявлена")
        spec = StageSpec(
            name=name,
            handler=handler,
            workers=workers,
            buffer=buffer,
            key=key,
            retry=retry,
            skip_on=skip_on or {},
            error_on=error_on or {},
            abort_if=_normalize_breakers(abort_if),
            limits=tuple(limits) if limits is not None else (),
            log_levels=log_levels or {},
            executor=executor,
            warn_queued=warn_queued,
        )
        stage = Stage(spec)
        self._stages[name] = stage
        return stage

    @property
    def clock(self) -> Clock:
        """Часы пайплайна."""
        return self._clock

    @property
    def started(self) -> bool:
        """Запущен ли пайплайн (есть реестр счётчиков)."""
        return self._ledger is not None

    def counts(self, name: str) -> StageCounts:
        """Счётчики стадии после (или во время) прогона."""
        if self._ledger is None:
            raise RuntimeError("пайплайн ещё не запускался")
        return self._ledger.counts(name)

    async def run(self, timeout: float | None = None) -> PipelineReport:  # noqa: ASYNC109, C901  # timeout — §4.7; run — оркестрация
        """Запустить пайплайн и дождаться завершения всех компонент.

        Args:
            timeout: мягкий лимит; по истечении — cancel (§4.7).

        Returns:
            Итоговый отчёт (последний снимок + длительность + причина остановки).

        Raises:
            RuntimeError: повторный запуск.
            TopologyError: граф не прошёл проверки (§7.6).
            BaseException: первый сбой воркера (плоско, с заметкой).
        """
        if self._ran:
            raise RuntimeError("Pipeline уже запускался: он одноразовый")
        self._ran = True

        specs: list[StageSpec] = [s.spec for s in self._stages.values()]
        for warning in validate_topology(specs):
            self._log.warning("стадия %s: %s", warning.stage, warning.message)

        for stage in self._stages.values():
            stage.inbox = Inbox(self._clock, maxsize=stage.spec.buffer)
            stage.tracker = EwmaTracker(self._clock)
            self._trackers[stage.spec.name] = stage.tracker

        def on_close(members: frozenset[str]) -> None:
            for member in members:
                member_inbox = self._stages[member].inbox
                if member_inbox is not None:
                    member_inbox.close()
                self._observer.stage_closed(member)

        seed_inputs = {
            name: (1 if stage.has_auto_seed else 0) + len(stage.feeders)
            for name, stage in self._stages.items()
        }
        ledger = Ledger(specs, seed_inputs=seed_inputs, on_close=on_close)
        self._ledger = ledger

        # Дозревший отложенный элемент возвращается в очередь (deferred → queued).
        for name, stage in self._stages.items():
            if stage.inbox is not None:

                def promote(_: Envelope[object], stage_name: str = name) -> None:
                    ledger.transition(stage_name, Event.REDELIVER)

                stage.inbox.set_on_promote(promote)
            for feeder in stage.feeders:
                feeder.bind(ledger, self._stages)

        await self._inject_seeds(ledger)
        ledger.check_all()

        supervisor = Supervisor()
        self._supervisor = supervisor
        factories = self._worker_factories(ledger, supervisor)
        start = self._clock.now()
        if timeout is None:
            await supervisor.run(factories)
        else:
            try:
                async with asyncio.timeout(timeout):
                    await supervisor.run(factories)
            except TimeoutError:
                self._hard_cancel("timeout")
        return self._report(self._clock.now() - start)

    def _report(self, duration: float) -> PipelineReport:
        """Построить итоговый отчёт по текущему состоянию реестра."""
        return PipelineReport(view=self.view(), duration=duration, stopped=self.stopped)

    def view(self) -> PipelineView:
        """Синхронный дешёвый снимок пайплайна (§4.8)."""
        if self._ledger is None:
            raise RuntimeError("пайплайн ещё не запускался")
        specs = [s.spec for s in self._stages.values()]
        expects = {name: stage.expect_hint for name, stage in self._stages.items()}
        failed = self._supervisor is not None and self._supervisor.failure is not None
        return build_pipeline_view(
            self.name,
            specs,
            self._ledger,
            self._trackers,
            expects,
            stopped=self.stopped,
            failed=failed,
        )

    async def watch(self, interval: float = 1.0) -> "AsyncIterator[PipelineView]":
        """Поток снимков раз в `interval`; заканчивается терминальным снимком (§4.8)."""
        while True:
            view = self.view()
            yield view
            if all(
                s.state in (StageState.CLOSED, StageState.CANCELLED, StageState.FAILED)
                for s in view.stages.values()
            ):
                return
            await self._clock.sleep(interval)

    def cancel(self, reason: str = "cancel") -> None:
        """Немедленно остановить: воркеры отменены, остаток → cancelled (§4.7).

        Идемпотентно. После `cancel`/`drain` `run()` завершается с
        `pipe.stopped`, а не бросает исключение.
        """
        if self.stopped is not None:
            return
        self.stopped = "cancel"
        self._log.info("пайплайн %s отменён: %s", self.name, reason)
        self._cancel_queues()
        if self._supervisor is not None:
            self._supervisor.cancel_all()

    async def drain(self, timeout: float | None = None) -> None:  # noqa: ASYNC109  # timeout — доменный параметр §4.7
        """Мягко остановить: входы закрыты, in-flight доделываются, остаток → cancelled.

        queued/deferred отменяются сразу; in-flight доигрывают. По истечении
        `timeout` (если воркеры не завершились) — переход в `cancel()`. Идемпотентно.
        """
        if self.stopped is not None:
            return
        self.stopped = "drain"
        self._log.info("пайплайн %s в режиме drain", self.name)
        for stage in self._stages.values():
            for feeder in stage.feeders:
                feeder.close()
        self._cancel_queues()
        if timeout is not None and self._supervisor is not None:
            await self._drain_watch(timeout)

    async def _drain_watch(self, timeout: float) -> None:  # noqa: ASYNC109  # эскалация drain→cancel
        """Подождать `timeout`; если воркеры ещё живы — перейти в cancel."""
        supervisor = self._supervisor
        if supervisor is None:  # pragma: no cover
            return
        await self._clock.sleep(timeout)
        if any(not task.done() for task in supervisor.tasks):
            self._hard_cancel("drain-timeout")

    def _hard_cancel(self, reason: str) -> None:
        """Жёсткая отмена (из cancel/таймаута), не трогая уже выставленный stopped."""
        if self.stopped is None:
            self.stopped = "cancel"
        self._log.info("пайплайн %s жёстко остановлен: %s", self.name, reason)
        self._cancel_queues()
        if self._supervisor is not None:
            self._supervisor.cancel_all()

    def _cancel_queues(self) -> None:
        """Учесть очереди (queued+deferred) как cancelled и опустошить входы."""
        if self._ledger is None:
            return
        for name, stage in self._stages.items():
            inbox = stage.inbox
            if inbox is None:
                continue
            ready, deferred = inbox.len_ready, inbox.len_deferred
            if ready:
                self._ledger.transition(name, Event.CANCEL_QUEUED, count=ready)
            if deferred:
                self._ledger.transition(name, Event.CANCEL_DEFERRED, count=deferred)
            inbox.cancel()

    async def _inject_seeds(self, ledger: Ledger) -> None:
        """Засеять входы (через dedup/resume) и запечатать авто-seed-входы."""
        for name, stage in self._stages.items():
            for item in stage.seed_items:
                await offer(stage, item, ledger=ledger, stages=self._stages, nowait=True)
            if stage.has_auto_seed:
                ledger.seal_seed(name)

    def _worker_factories(
        self,
        ledger: Ledger,
        supervisor: Supervisor,
    ) -> list[Callable[[], Coroutine[object, object, None]]]:
        """Построить фабрики корутин-воркеров (по `workers` на стадию)."""
        factories: list[Callable[[], Coroutine[object, object, None]]] = []
        for stage in self._stages.values():
            for _ in range(stage.spec.workers):

                def factory(stage: Stage = stage) -> Coroutine[object, object, None]:
                    return run_worker(
                        stage,
                        clock=self._clock,
                        stages=self._stages,
                        ledger=ledger,
                        log=self._log,
                        observer=self._observer,
                        supervisor=supervisor,
                    )

                factories.append(factory)
        return factories
