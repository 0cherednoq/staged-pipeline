"""Тесты часов: FakeClock (порядок пробуждения, advance, отмена) и MonotonicClock (T1.1)."""

import asyncio

import pytest

from staged_pipeline._clock import Clock, FakeClock, MonotonicClock


async def _let_tasks_register(count: int, clock: FakeClock) -> None:
    """Прокрутить цикл, пока `count` корутин не зарегистрируют сон."""
    for _ in range(count + 1):
        if len(clock._sleepers) >= count:
            return
        await asyncio.sleep(0)


async def test_wake_order_by_deadline() -> None:
    clock = FakeClock()
    woke: list[str] = []

    async def sleeper(name: str, delay: float) -> None:
        await clock.sleep(delay)
        woke.append(name)

    tasks = [
        asyncio.create_task(sleeper("a", 3)),
        asyncio.create_task(sleeper("b", 1)),
        asyncio.create_task(sleeper("c", 2)),
    ]
    await _let_tasks_register(3, clock)

    clock.advance(3)
    await asyncio.gather(*tasks)

    assert woke == ["b", "c", "a"]
    assert clock.now() == 3


async def test_equal_deadlines_wake_in_sleep_order() -> None:
    clock = FakeClock()
    woke: list[int] = []

    async def sleeper(index: int) -> None:
        await clock.sleep(1)
        woke.append(index)

    tasks = [asyncio.create_task(sleeper(i)) for i in range(3)]
    await _let_tasks_register(3, clock)

    clock.advance(1)
    await asyncio.gather(*tasks)

    assert woke == [0, 1, 2]


async def test_advance_zero_wakes_zero_delay_sleeper() -> None:
    clock = FakeClock()
    woke: list[bool] = []

    async def sleeper() -> None:
        await clock.sleep(0)
        woke.append(True)

    task = asyncio.create_task(sleeper())
    await _let_tasks_register(1, clock)
    assert woke == []  # время не двигали — спит

    clock.advance(0)
    await task
    assert woke == [True]
    assert clock.now() == 0


async def test_advance_partial_does_not_wake_later_sleeper() -> None:
    clock = FakeClock()
    woke: list[bool] = []

    async def sleeper() -> None:
        await clock.sleep(5)
        woke.append(True)

    task = asyncio.create_task(sleeper())
    await _let_tasks_register(1, clock)

    clock.advance(3)
    await asyncio.sleep(0)
    assert woke == []
    assert clock.now() == 3

    clock.advance(2)
    await task
    assert woke == [True]


async def test_negative_advance_rejected() -> None:
    clock = FakeClock()
    with pytest.raises(ValueError, match="отрицательн"):
        clock.advance(-1)


async def test_cancel_removes_only_cancelled_sleeper() -> None:
    clock = FakeClock()
    survived: list[str] = []

    async def cancellable() -> None:
        await clock.sleep(5)

    async def survivor() -> None:
        await clock.sleep(5)
        survived.append("ok")

    doomed = asyncio.create_task(cancellable())
    keeper = asyncio.create_task(survivor())
    await _let_tasks_register(2, clock)

    doomed.cancel()
    with pytest.raises(asyncio.CancelledError):
        await doomed

    # В куче остался только выживший спящий.
    assert len(clock._sleepers) == 1
    clock.advance(10)
    await keeper
    assert survived == ["ok"]


async def test_fake_clock_start_value() -> None:
    clock = FakeClock(start=100.0)
    assert clock.now() == 100.0
    clock.advance(1.5)
    assert clock.now() == 101.5


async def test_monotonic_clock_now_and_sleep() -> None:
    clock = MonotonicClock()
    before = clock.now()
    assert isinstance(before, float)
    await clock.sleep(0.001)
    assert clock.now() >= before


async def test_monotonic_clock_negative_sleep_is_immediate() -> None:
    clock = MonotonicClock()
    await clock.sleep(-1)  # не должно зависать


def test_clock_runtime_checkable() -> None:
    assert isinstance(MonotonicClock(), Clock)
    assert isinstance(FakeClock(), Clock)
