"""Тесты интеграции с Tallyho: track (прогресс/incr/drain) и outcome_for (T5.1)."""

import asyncio
from collections.abc import Callable

from staged_pipeline._clock import FakeClock
from staged_pipeline._pipeline import Pipeline
from staged_pipeline.integrations.tallyho import TallyhoItemLike, outcome_for, track

TIMEOUT = 3.0


async def _until(predicate: Callable[[], bool]) -> None:
    for _ in range(2000):
        await asyncio.sleep(0)
        if predicate():
            return
    msg = "условие не выполнилось"
    raise AssertionError(msg)


class FakeItem:
    def __init__(self, *, cancelled: bool = False) -> None:
        self.progress_calls: list[tuple[int, int | None]] = []
        self.metrics: dict[str, int] = {}
        self._cancelled = cancelled

    def progress(self, done: int, total: int | None = None) -> None:
        self.progress_calls.append((done, total))

    def incr(self, name: str, value: int = 1) -> None:
        self.metrics[name] = self.metrics.get(name, 0) + value

    def cancelled(self) -> bool:
        return self._cancelled


def test_fake_item_is_tallyho_item_like() -> None:
    assert isinstance(FakeItem(), TallyhoItemLike)


async def test_track_reports_progress_every_interval() -> None:
    clock = FakeClock()
    item = FakeItem()
    gate = asyncio.Event()

    async def work(n: int) -> None:
        if n == 1:
            await gate.wait()

    pipe = Pipeline("t", clock=clock)
    pipe.stage("s", work, workers=1).seed([1, 2, 3])

    async with track(pipe, item, every=1.0):
        run_task = asyncio.create_task(pipe.run())
        await _until(lambda: pipe.started and pipe.counts("s").in_flight == 1)
        await _until(lambda: bool(clock._sleepers))  # монитор уснул на интервале
        clock.advance(1.0)
        await _until(lambda: len(item.progress_calls) >= 1)
        assert item.metrics == {}  # incr ещё не было — только на выходе
        gate.set()
        await asyncio.wait_for(run_task, TIMEOUT)

    # На выходе выгружены итоговые счётчики.
    assert item.metrics.get("s.ok") == 3
    assert item.progress_calls


async def test_track_cancelled_triggers_drain() -> None:
    clock = FakeClock()
    item = FakeItem(cancelled=True)
    gate = asyncio.Event()

    async def work(n: int) -> None:
        if n == 1:
            await gate.wait()

    pipe = Pipeline("c", clock=clock)
    pipe.stage("s", work, workers=1).seed([1, 2, 3])

    async with track(pipe, item, every=1.0):
        run_task = asyncio.create_task(pipe.run())
        await _until(lambda: pipe.started and pipe.counts("s").in_flight == 1)
        await _until(lambda: bool(clock._sleepers))
        clock.advance(1.0)  # монитор видит cancelled → drain
        await _until(lambda: pipe.stopped == "drain")
        gate.set()
        await asyncio.wait_for(run_task, TIMEOUT)

    assert pipe.stopped == "drain"
    assert pipe.counts("s").cancelled == 2  # очередь 2,3 отменена


async def test_outcome_for_exported_and_partial() -> None:
    async def work(n: int) -> None:  # noqa: ARG001
        return None

    pipe = Pipeline("o")
    pipe.stage("s", work).seed([1, 2])
    report = await asyncio.wait_for(pipe.run(), TIMEOUT)

    assert outcome_for(report) == ("ok", "exported")
    assert outcome_for(report, partial_if=lambda _: True) == ("ok", "partial")
