"""Тесты лимитов: общая конкурентность, отсутствие deadlock, вес, частота (T3.4)."""

import asyncio
from collections.abc import Callable

from staged_pipeline._clock import FakeClock
from staged_pipeline._limit import Limit
from staged_pipeline._pipeline import Pipeline

TIMEOUT = 3.0


async def _until(predicate: Callable[[], bool]) -> None:
    for _ in range(2000):
        await asyncio.sleep(0)
        if predicate():
            return
    msg = "условие не выполнилось"
    raise AssertionError(msg)


async def test_shared_concurrency_limit_not_exceeded() -> None:
    limit = Limit(concurrency=2)
    state = {"current": 0, "peak": 0}

    async def track_a(n: int) -> int:
        state["current"] += 1
        state["peak"] = max(state["peak"], state["current"])
        await asyncio.sleep(0.005)
        state["current"] -= 1
        return n

    async def track_b(n: int) -> None:  # noqa: ARG001
        state["current"] += 1
        state["peak"] = max(state["peak"], state["current"])
        await asyncio.sleep(0.005)
        state["current"] -= 1

    pipe = Pipeline("conc")
    a = pipe.stage("a", track_a, workers=4, limits=[limit])
    b = pipe.stage("b", track_b, workers=4, limits=[limit])
    a.to(b)
    a.seed(list(range(12)))

    await asyncio.wait_for(pipe.run(), TIMEOUT)
    assert state["peak"] <= 2  # общий лимит на две стадии


async def test_no_deadlock_limit_one_with_bounded_downstream() -> None:
    limit = Limit(concurrency=1)
    got: list[int] = []

    async def produce(n: int) -> int:
        return n

    async def consume(n: int) -> None:
        got.append(n)

    pipe = Pipeline("deadlock")
    a = pipe.stage("a", produce, workers=1, limits=[limit])
    b = pipe.stage("b", consume, workers=1, buffer=1, limits=[limit])
    a.to(b)
    a.seed([1, 2, 3])

    # Если бы permit удерживался на emit — вечный deadlock; таймаут его поймал бы.
    await asyncio.wait_for(pipe.run(), TIMEOUT)
    assert sorted(got) == [1, 2, 3]


async def test_weighted_limit() -> None:
    limit = Limit(concurrency=4)
    state = {"current": 0, "peak": 0}

    async def heavy(n: int) -> None:  # noqa: ARG001
        state["current"] += 1
        state["peak"] = max(state["peak"], state["current"])
        await asyncio.sleep(0.005)
        state["current"] -= 1

    pipe = Pipeline("weight")
    # weight=2 на лимите 4 → не более 2 одновременно.
    pipe.stage("h", heavy, workers=4, limits=[limit.weighted(2)]).seed(list(range(8)))
    await asyncio.wait_for(pipe.run(), TIMEOUT)
    assert state["peak"] <= 2


async def test_rate_limit_spaces_calls() -> None:
    clock = FakeClock()
    limit = Limit(rate=2, per=1.0, clock=clock)
    processed: list[int] = []

    async def work(n: int) -> None:
        processed.append(n)

    pipe = Pipeline("rate", clock=clock)
    pipe.stage("s", work, workers=1, limits=[limit]).seed([1, 2, 3, 4])

    task = asyncio.create_task(pipe.run())
    # Первые 2 проходят сразу (бакет полон), затем воркер ждёт пополнения.
    await _until(lambda: len(processed) == 2)
    await _until(lambda: bool(clock._sleepers))
    clock.advance(1.0)  # +2 токена
    await _until(lambda: len(processed) == 4)
    await asyncio.wait_for(task, TIMEOUT)
    assert processed == [1, 2, 3, 4]
