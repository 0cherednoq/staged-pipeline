"""Регрессионные тесты исправлений аудита (docs/AUDIT.md)."""

import asyncio
from collections.abc import Callable

from staged_pipeline._pipeline import Pipeline

TIMEOUT = 3.0


async def _until(predicate: Callable[[], bool]) -> None:
    for _ in range(2000):
        await asyncio.sleep(0)
        if predicate():
            return
    msg = "условие не выполнилось"
    raise AssertionError(msg)


async def test_v2_drain_with_bounded_put_is_graceful() -> None:
    """V2: drain при воркере, висящем на bounded put, — это остановка, а не сбой."""
    release = asyncio.Event()

    async def produce(n: int) -> int:
        return n

    async def consume(n: int) -> None:
        if n == 0:
            await release.wait()  # держим in-flight, очередь downstream забивается

    pipe = Pipeline("v2")
    a = pipe.stage("a", produce, workers=1)
    b = pipe.stage("b", consume, workers=1, buffer=1)
    a.to(b)
    a.seed(list(range(10)))

    task = asyncio.create_task(pipe.run())
    # Ждём, пока upstream зависнет на put в полный bounded-downstream.
    await _until(lambda: pipe.started and pipe.counts("a").in_flight == 1)
    await _until(lambda: pipe.counts("b").queued >= 1)

    await pipe.drain()
    release.set()
    report = await asyncio.wait_for(task, TIMEOUT)

    # Главное: не упало, а корректно остановилось.
    assert report.stopped == "drain"
    assert pipe.counts("a").invariant_holds()
    assert pipe.counts("b").invariant_holds()


async def test_v4_observer_exception_does_not_kill_pipeline() -> None:
    """V4: падающий Observer не роняет прогон."""

    class BrokenObserver:
        def stage_closed(self, stage: str) -> None:
            raise RuntimeError("boom-closed")

        def item_finished(self, stage: str, key: object, outcome: str, exc: object) -> None:
            raise RuntimeError("boom-finished")

        def breaker_tripped(self, stage: str, reason: str) -> None:
            raise RuntimeError("boom-breaker")

        def queued_warning(self, stage: str, queued: int) -> None:
            raise RuntimeError("boom-queued")

    async def handler(n: int) -> None:  # noqa: ARG001
        return None

    pipe = Pipeline("v4", observer=BrokenObserver())
    pipe.stage("s", handler).seed([1, 2, 3])
    report = await asyncio.wait_for(pipe.run(), TIMEOUT)

    assert report.stopped is None
    assert report.stages["s"].ok == 3


async def test_v4_queued_warning_observer_exception_swallowed() -> None:
    """V4: падение queued_warning тоже не фатально."""
    calls: list[int] = []

    class WarnObserver:
        def stage_closed(self, stage: str) -> None: ...
        def item_finished(self, stage: str, key: object, outcome: str, exc: object) -> None: ...
        def breaker_tripped(self, stage: str, reason: str) -> None: ...
        def queued_warning(self, stage: str, queued: int) -> None:
            calls.append(queued)
            raise RuntimeError("boom")

    async def handler(n: int) -> None:  # noqa: ARG001
        return None

    pipe = Pipeline("v4b", observer=WarnObserver())
    pipe.stage("s", handler, workers=1, warn_queued=2).seed(list(range(10)))
    report = await asyncio.wait_for(pipe.run(), TIMEOUT)

    assert report.stages["s"].ok == 10
    assert calls  # предупреждение сработало, но исключение проглочено
