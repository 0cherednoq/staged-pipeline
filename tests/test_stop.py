"""Тесты остановки: cancel, drain, таймаут, внешняя отмена (T3.3)."""

import asyncio
from collections.abc import Callable

import pytest

from staged_pipeline._clock import FakeClock
from staged_pipeline._pipeline import Pipeline

TIMEOUT = 3.0


async def _until(predicate: Callable[[], bool]) -> None:
    for _ in range(2000):
        await asyncio.sleep(0)
        if predicate():
            return
    msg = "условие не выполнилось"
    raise AssertionError(msg)


def _in_flight(pipe: Pipeline, stage: str) -> bool:
    return pipe._ledger is not None and pipe.counts(stage).in_flight >= 1


async def test_cancel_marks_remainder_cancelled() -> None:
    gate = asyncio.Event()

    async def slow(n: int) -> None:  # noqa: ARG001
        await gate.wait()

    pipe = Pipeline("cancel")
    pipe.stage("s", slow, workers=1).seed([1, 2, 3])
    task = asyncio.create_task(pipe.run())
    await _until(lambda: _in_flight(pipe, "s"))

    pipe.cancel("test")
    await asyncio.wait_for(task, TIMEOUT)
    assert pipe.stopped == "cancel"
    assert pipe.counts("s").cancelled == 2  # queued 2 и 3


async def test_cancel_is_idempotent() -> None:
    gate = asyncio.Event()

    async def slow(n: int) -> None:  # noqa: ARG001
        await gate.wait()

    pipe = Pipeline("idem")
    pipe.stage("s", slow, workers=1).seed([1, 2])
    task = asyncio.create_task(pipe.run())
    await _until(lambda: _in_flight(pipe, "s"))
    pipe.cancel("first")
    pipe.cancel("second")  # no-op
    await asyncio.wait_for(task, TIMEOUT)
    assert pipe.stopped == "cancel"


async def test_drain_finishes_inflight_not_queued() -> None:
    release = asyncio.Event()
    done: list[int] = []

    async def work(n: int) -> None:
        if n == 1:
            await release.wait()
        done.append(n)

    pipe = Pipeline("drain")
    pipe.stage("s", work, workers=1).seed([1, 2, 3])
    task = asyncio.create_task(pipe.run())
    await _until(lambda: _in_flight(pipe, "s"))

    await pipe.drain()  # queued 2,3 → cancelled; in-flight 1 доигрывает
    release.set()
    await asyncio.wait_for(task, TIMEOUT)

    assert pipe.stopped == "drain"
    assert done == [1]
    assert pipe.counts("s").ok == 1
    assert pipe.counts("s").cancelled == 2


async def test_drain_timeout_escalates_to_cancel() -> None:
    clock = FakeClock()

    async def stuck(n: int) -> None:  # noqa: ARG001
        await asyncio.Event().wait()

    pipe = Pipeline("draintimeout", clock=clock)
    pipe.stage("s", stuck, workers=1).seed([1, 2])
    task = asyncio.create_task(pipe.run())
    await _until(lambda: _in_flight(pipe, "s"))

    drain_task = asyncio.create_task(pipe.drain(timeout=5))
    await _until(lambda: bool(clock._sleepers))  # watcher drain уснул
    clock.advance(5)  # таймаут drain → cancel
    await asyncio.wait_for(drain_task, TIMEOUT)
    await asyncio.wait_for(task, TIMEOUT)
    assert pipe.stopped == "drain"


async def test_external_cancel_propagates_without_hanging() -> None:
    async def stuck(n: int) -> None:  # noqa: ARG001
        await asyncio.Event().wait()

    pipe = Pipeline("external")
    pipe.stage("s", stuck, workers=1).seed([1])
    task = asyncio.create_task(pipe.run())
    await _until(lambda: _in_flight(pipe, "s"))

    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


async def test_run_timeout_cancels() -> None:
    clock = FakeClock()

    async def stuck(n: int) -> None:  # noqa: ARG001
        await asyncio.Event().wait()

    pipe = Pipeline("runtimeout", clock=clock)
    pipe.stage("s", stuck, workers=1).seed([1])
    # run(timeout) использует asyncio.timeout по реальному loop-времени; берём малый.
    await asyncio.wait_for(pipe.run(timeout=0.05), TIMEOUT)
    assert pipe.stopped == "cancel"
