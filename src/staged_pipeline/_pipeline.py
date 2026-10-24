"""Сборка и запуск пайплайна: `Pipeline`, `Pipeline.stage`, `seed`, `run` (§4.1, §7.3).

`Pipeline` — граф + супервизор + часы, одноразовый (один `run()`). `run()` проверяет
топологию до старта, засевает входы, запускает по пулу воркеров на стадию в одной
TaskGroup и ждёт естественного завершения (закрытие всех компонент по §7.2).
"""

import logging
from collections.abc import Callable, Coroutine, Mapping, Sequence

from staged_pipeline._accounting import Event, Ledger, StageCounts
from staged_pipeline._clock import Clock, MonotonicClock
from staged_pipeline._errors import ConfigurationError
from staged_pipeline._graph import KeyFn, StageSpec, validate_topology
from staged_pipeline._inbox import Envelope, Inbox
from staged_pipeline._policy import Breaker, ExcMap, Retry
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
    ) -> None:
        """Создать пайплайн.

        Args:
            name: имя (для логов).
            clock: часы; по умолчанию `MonotonicClock`.
            log: логгер; по умолчанию `staged_pipeline`.
        """
        self.name: str = name
        self._clock: Clock = clock or MonotonicClock()
        self._log: logging.Logger = log or _log
        self._stages: dict[str, Stage] = {}
        self._ran: bool = False
        self._ledger: Ledger | None = None

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
        log_levels: Mapping[str, int] | None = None,
        executor: str | None = None,
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
            log_levels=log_levels or {},
            executor=executor,
        )
        stage = Stage(spec)
        self._stages[name] = stage
        return stage

    def counts(self, name: str) -> StageCounts:
        """Счётчики стадии после (или во время) прогона."""
        if self._ledger is None:
            raise RuntimeError("пайплайн ещё не запускался")
        return self._ledger.counts(name)

    async def run(self, timeout: float | None = None) -> None:  # noqa: ASYNC109, C901  # таймаут — T3.3; run — оркестрация
        """Запустить пайплайн и дождаться завершения всех компонент.

        Args:
            timeout: пока не используется (реализуется в T3.3).

        Raises:
            RuntimeError: повторный запуск.
            TopologyError: граф не прошёл проверки (§7.6).
            BaseException: первый сбой воркера (плоско, с заметкой).
        """
        if self._ran:
            raise RuntimeError("Pipeline уже запускался: он одноразовый")
        self._ran = True
        if timeout is not None:
            self._log.debug(
                "timeout=%s игнорируется в текущей версии (реализуется в T3.3)", timeout
            )

        specs: list[StageSpec] = [s.spec for s in self._stages.values()]
        for warning in validate_topology(specs):
            self._log.warning("стадия %s: %s", warning.stage, warning.message)

        for stage in self._stages.values():
            stage.inbox = Inbox(self._clock, maxsize=stage.spec.buffer)

        def on_close(members: frozenset[str]) -> None:
            for member in members:
                member_inbox = self._stages[member].inbox
                if member_inbox is not None:
                    member_inbox.close()

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
        factories = self._worker_factories(ledger, supervisor)
        await supervisor.run(factories)

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
                        supervisor=supervisor,
                    )

                factories.append(factory)
        return factories
