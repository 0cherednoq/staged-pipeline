"""Тесты входа стадии Inbox: готовые, отложенные, close/cancel, bounded (T1.3)."""

import asyncio
from collections.abc import Callable

import pytest

from staged_pipeline._clock import FakeClock
from staged_pipeline._errors import StageClosedError
from staged_pipeline._inbox import Envelope, Inbox


async def _until(predicate: Callable[[], bool]) -> None:
    """Прокрутить цикл, пока условие не выполнится (детерминировано в одном loop)."""
    for _ in range(1000):
        if predicate():
            return
        await asyncio.sleep(0)
    msg = "условие так и не выполнилось"
    raise AssertionError(msg)


async def test_fifo_order() -> None:
    inbox: Inbox[str] = Inbox(FakeClock())
    inbox.put_nowait(Envelope("a"))
    inbox.put_nowait(Envelope("b"))
    first = await inbox.get()
    second = await inbox.get()
    assert first is not None
    assert second is not None
    assert (first.item, second.item) == ("a", "b")
    assert inbox.len_ready == 0


async def test_get_waits_then_wakes_on_put() -> None:
    inbox: Inbox[str] = Inbox(FakeClock())
    got: list[Envelope[str] | None] = []

    async def consumer() -> None:
        got.append(await inbox.get())

    task = asyncio.create_task(consumer())
    await _until(lambda: len(inbox._get_waiters) == 1)
    inbox.put_nowait(Envelope("late"))
    await task
    assert got[0] is not None
    assert got[0].item == "late"


async def test_deferred_promoted_in_not_before_order() -> None:
    clock = FakeClock()
    inbox: Inbox[int] = Inbox(clock)
    inbox.defer(Envelope(3), not_before=3)
    inbox.defer(Envelope(1), not_before=1)
    inbox.defer(Envelope(2), not_before=2)

    clock.advance(3)
    items = [await inbox.get(), await inbox.get(), await inbox.get()]
    assert [e.item for e in items if e is not None] == [1, 2, 3]


async def test_deferred_wakes_waiting_getter_by_clock() -> None:
    clock = FakeClock()
    inbox: Inbox[str] = Inbox(clock)
    got: list[Envelope[str] | None] = []

    async def consumer() -> None:
        got.append(await inbox.get())

    inbox.defer(Envelope("x"), not_before=5)
    task = asyncio.create_task(consumer())
    await _until(lambda: len(clock._sleepers) == 1)  # getter уснул до дедлайна

    clock.advance(5)
    await task
    assert got[0] is not None
    assert got[0].item == "x"


async def test_partial_advance_keeps_getter_waiting() -> None:
    clock = FakeClock()
    inbox: Inbox[str] = Inbox(clock)
    got: list[Envelope[str] | None] = []

    async def consumer() -> None:
        got.append(await inbox.get())

    inbox.defer(Envelope("x"), not_before=5)
    task = asyncio.create_task(consumer())
    await _until(lambda: len(clock._sleepers) == 1)

    clock.advance(3)
    await _until(lambda: len(clock._sleepers) == 1)  # снова уснул до дедлайна
    assert got == []

    clock.advance(2)
    await task
    assert got[0] is not None


async def test_put_wakes_getter_and_cancels_deferred_timer() -> None:
    clock = FakeClock()
    inbox: Inbox[str] = Inbox(clock)
    got: list[Envelope[str] | None] = []

    async def consumer() -> None:
        got.append(await inbox.get())

    inbox.defer(Envelope("later"), not_before=100)
    task = asyncio.create_task(consumer())
    await _until(lambda: len(clock._sleepers) == 1)  # ждёт на таймере отложенного

    inbox.put_nowait(Envelope("now"))  # будит через waiter, таймер отменяется
    await task
    assert got[0] is not None
    assert got[0].item == "now"
    assert inbox.len_deferred == 1  # отложенный остался


async def test_close_wakes_waiting_getter_with_none() -> None:
    inbox: Inbox[str] = Inbox(FakeClock())
    got: list[Envelope[str] | None] = []

    async def consumer() -> None:
        got.append(await inbox.get())

    task = asyncio.create_task(consumer())
    await _until(lambda: len(inbox._get_waiters) == 1)
    inbox.close()
    await task
    assert got == [None]


async def test_close_drains_ready_then_none() -> None:
    inbox: Inbox[str] = Inbox(FakeClock())
    inbox.put_nowait(Envelope("a"))
    inbox.close()
    first = await inbox.get()
    assert first is not None
    assert first.item == "a"
    assert await inbox.get() is None


async def test_cancel_returns_remainder_and_closes() -> None:
    clock = FakeClock()
    inbox: Inbox[str] = Inbox(clock)
    inbox.put_nowait(Envelope("r1"))
    inbox.put_nowait(Envelope("r2"))
    inbox.defer(Envelope("d_late"), not_before=10)
    inbox.defer(Envelope("d_soon"), not_before=5)

    remainder = inbox.cancel()
    assert [e.item for e in remainder] == ["r1", "r2", "d_soon", "d_late"]
    assert inbox.len_ready == 0
    assert inbox.len_deferred == 0
    assert await inbox.get() is None


@pytest.mark.parametrize("closed_by", ["close", "cancel"])
async def test_put_into_closed_raises(closed_by: str) -> None:
    inbox: Inbox[str] = Inbox(FakeClock())
    if closed_by == "close":
        inbox.close()
    else:
        inbox.cancel()

    with pytest.raises(StageClosedError):
        inbox.put_nowait(Envelope("x"))
    with pytest.raises(StageClosedError):
        await inbox.put(Envelope("x"))
    with pytest.raises(StageClosedError):
        inbox.defer(Envelope("x"), not_before=1)


async def test_bounded_put_waits_for_space() -> None:
    inbox: Inbox[str] = Inbox(FakeClock(), maxsize=1)
    await inbox.put(Envelope("a"))  # заполнили

    blocked = asyncio.create_task(inbox.put(Envelope("b")))
    await _until(lambda: len(inbox._put_waiters) == 1)
    assert inbox.len_ready == 1

    taken = await inbox.get()  # освободили место → разбудили put
    assert taken is not None
    assert taken.item == "a"
    await blocked
    assert inbox.len_ready == 1


async def test_bounded_put_unblocked_by_close_raises() -> None:
    inbox: Inbox[str] = Inbox(FakeClock(), maxsize=1)
    await inbox.put(Envelope("a"))
    blocked = asyncio.create_task(inbox.put(Envelope("b")))
    await _until(lambda: len(inbox._put_waiters) == 1)

    inbox.close()
    with pytest.raises(StageClosedError):
        await blocked


async def test_unbounded_put_does_not_block() -> None:
    inbox: Inbox[int] = Inbox(FakeClock())
    for i in range(100):
        await inbox.put(Envelope(i))
    assert inbox.len_ready == 100
    assert not inbox.closed


async def test_closed_property() -> None:
    inbox: Inbox[int] = Inbox(FakeClock())
    assert not inbox.closed
    inbox.close()
    assert inbox.closed
