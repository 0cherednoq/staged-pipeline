"""Адаптер прогресса и остановки для Tallyho (architecture.md §9.3).

Мост через структурный `Protocol` — `import tallyho` не требуется и запрещён
(контракт import-linter). `track` раз в `every` шлёт живой прогресс в `item`, на
выходе — итоговые счётчики, и переводит пайплайн в `drain`, когда `item.cancelled()`.
`outcome_for` возвращает простой кортеж, который приложение само заворачивает в
`tallyho.Ok/...`.
"""

import asyncio
import contextlib
from collections.abc import AsyncGenerator, Callable, Mapping
from typing import Protocol, runtime_checkable

from staged_pipeline._pipeline import Pipeline
from staged_pipeline._progress import PipelineReport, PipelineView

__all__ = ["TallyhoItemLike", "outcome_for", "track"]


@runtime_checkable
class TallyhoItemLike(Protocol):
    """Минимальный интерфейс `WorkItem`, нужный адаптеру (architecture.md §9.3)."""

    def progress(self, done: int, total: int | None = None) -> None:
        """Сообщить живой прогресс аккаунта."""
        ...

    def incr(self, name: str, value: int = 1) -> None:
        """Накопить итоговую метрику."""
        ...

    def cancelled(self) -> bool:
        """Запрошена ли кооперативная отмена."""
        ...


def _progress_args(view: PipelineView, weights: Mapping[str, int]) -> tuple[int, int | None]:
    """Посчитать взвешенные (done, total) по стадиям; total=None, если не у всех есть expected."""
    done = 0
    total = 0
    total_known = True
    for name, stage in view.stages.items():
        weight = weights.get(name, 1)
        done += weight * stage.done
        if stage.expected is None:
            total_known = False
        else:
            total += weight * stage.expected
    return done, (total if total_known else None)


def _push_metrics(view: PipelineView, item: TallyhoItemLike) -> None:
    """Выгрузить итоговые счётчики и метки стадий в `item.incr` (только на выходе)."""
    for name, stage in view.stages.items():
        for outcome, value in (
            ("ok", stage.ok),
            ("skip", stage.skip),
            ("error", stage.error),
            ("duplicates", stage.duplicates),
        ):
            if value:
                item.incr(f"{name}.{outcome}", value)
        for label, count in stage.labels.items():
            if count:
                item.incr(f"{name}:{label}", count)


@contextlib.asynccontextmanager
async def track(
    pipe: Pipeline,
    item: TallyhoItemLike,
    *,
    every: float = 1.0,
    drain_timeout: float | None = None,
    weights: Mapping[str, int] | None = None,
) -> AsyncGenerator[None]:
    """Мост прогресса и отмены на время прогона пайплайна (architecture.md §9.3).

    Раз в `every` секунд шлёт `item.progress`; при `item.cancelled()` переводит
    пайплайн в `pipe.drain(drain_timeout)`. На выходе выгружает итоговые счётчики
    в `item.incr` (один раз, как копит Tallyho).
    """
    resolved_weights = weights or {}
    stop = asyncio.Event()

    async def monitor() -> None:
        while not stop.is_set():
            if pipe.started:
                view = pipe.view()
                done, total = _progress_args(view, resolved_weights)
                item.progress(done, total)
                if item.cancelled():
                    await pipe.drain(drain_timeout)
                    return
            await _sleep_or_stop(pipe, stop, every)

    task = asyncio.create_task(monitor())
    try:
        yield
    finally:
        stop.set()
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task
        if pipe.started:
            _push_metrics(pipe.view(), item)


async def _sleep_or_stop(pipe: Pipeline, stop: asyncio.Event, every: float) -> None:
    """Подождать `every` по часам пайплайна или досрочной остановки."""
    waiter = asyncio.ensure_future(stop.wait())
    timer = asyncio.ensure_future(pipe.clock.sleep(every))
    try:
        await asyncio.wait({waiter, timer}, return_when=asyncio.FIRST_COMPLETED)
    finally:
        for fut in (waiter, timer):
            if not fut.done():
                fut.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await fut


def outcome_for(
    report: PipelineReport,
    *,
    partial_if: Callable[[PipelineReport], bool] | None = None,
    ok_label: str = "exported",
    partial_label: str = "partial",
) -> tuple[str, str]:
    """Вернуть исход как простой кортеж для обёртки приложением (`tallyho.Ok(...)`).

    Returns:
        `("ok", partial_label)` если задан `partial_if` и он истинен, иначе
        `("ok", ok_label)`.
    """
    if partial_if is not None and partial_if(report):
        return ("ok", partial_label)
    return ("ok", ok_label)
