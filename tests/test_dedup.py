"""Тесты dedup, resume и seed_open/Feeder (T3.1)."""

import asyncio
from collections.abc import Callable
from typing import cast

import pytest

from staged_pipeline._clock import FakeClock
from staged_pipeline._errors import StageClosedError
from staged_pipeline._pipeline import Pipeline
from staged_pipeline._policy import Fixed, Retry
from staged_pipeline._stage import SetSeenStore

TIMEOUT = 3.0


def _ident(value: object) -> object:
    return value


async def _until(predicate: Callable[[], bool]) -> None:
    for _ in range(1000):
        await asyncio.sleep(0)
        if predicate():
            return
    msg = "условие не выполнилось"
    raise AssertionError(msg)


def test_set_seen_store() -> None:
    store = SetSeenStore()
    assert store.add("a") is True
    assert store.add("a") is False
    assert "a" in store
    assert "b" not in store


async def test_dedup_same_key_from_two_folders() -> None:
    processed: list[tuple[str, str]] = []

    async def handler(item: tuple[str, str]) -> None:
        processed.append(item)

    def by_thread(item: object) -> object:
        return cast("tuple[str, str]", item)[1]

    pipe = Pipeline("dedup")
    pipe.stage("s", handler, key=by_thread).seed([("f1", "A"), ("f2", "A"), ("f1", "B")])
    await asyncio.wait_for(pipe.run(), TIMEOUT)

    counts = pipe.counts("s")
    assert counts.found == 2
    assert counts.duplicates == 1
    assert counts.ok == 2
    assert len(processed) == 2


async def test_resume_counts_as_found_and_skip() -> None:
    processed: list[int] = []

    async def handler(n: int) -> None:
        processed.append(n)

    pipe = Pipeline("resume")
    stage = pipe.stage("s", handler, key=_ident)
    stage.resume([1, 2], label="resumed")
    stage.seed([1, 2, 3])
    await asyncio.wait_for(pipe.run(), TIMEOUT)

    counts = pipe.counts("s")
    assert counts.found == 3
    assert counts.skip == 2
    assert counts.labels["resumed"] == 2
    assert counts.ok == 1
    assert processed == [3]


async def test_retry_does_not_repass_dedup() -> None:
    clock = FakeClock()
    attempts = {1: 1}

    async def handler(n: int) -> None:
        if attempts.get(n, 0) > 0:
            attempts[n] -= 1
            raise ValueError

    pipe = Pipeline("retrydedup", clock=clock)
    pipe.stage(
        "s",
        handler,
        key=_ident,
        retry=Retry(on=ValueError, attempts=2, backoff=Fixed(1.0)),
    ).seed([1])

    task = asyncio.create_task(pipe.run())
    await _until(lambda: pipe.counts("s").deferred == 1)
    await _until(lambda: bool(clock._sleepers))
    clock.advance(1)
    await asyncio.wait_for(task, TIMEOUT)

    counts = pipe.counts("s")
    assert counts.duplicates == 0  # ретрай не прошёл dedup повторно
    assert counts.ok == 1


async def test_feeder_holds_stage_open_and_delivers() -> None:
    got: list[int] = []

    async def handler(n: int) -> None:
        got.append(n)

    pipe = Pipeline("feeder")
    stage = pipe.stage("s", handler)
    feed = stage.seed_open()

    task = asyncio.create_task(pipe.run())
    await _until(lambda: pipe._ledger is not None)

    await feed.put(1)
    await feed.put(2)
    await _until(lambda: pipe.counts("s").ok == 2)
    # Стадия ещё открыта: Feeder держит writer.
    ledger = pipe._ledger
    assert ledger is not None
    assert not ledger.is_closed("s")

    feed.close()
    await asyncio.wait_for(task, TIMEOUT)
    assert got == [1, 2]


async def test_feeder_put_before_run_rejected() -> None:
    pipe = Pipeline("early")

    async def handler(n: int) -> None:  # noqa: ARG001
        return None

    feed = pipe.stage("s", handler).seed_open()
    with pytest.raises(RuntimeError, match="до запуска"):
        await feed.put(1)


async def test_feeder_put_after_close_rejected() -> None:
    got: list[int] = []

    async def handler(n: int) -> None:
        got.append(n)

    pipe = Pipeline("closed")
    feed = pipe.stage("s", handler).seed_open()
    task = asyncio.create_task(pipe.run())
    await _until(lambda: pipe._ledger is not None)
    feed.close()
    with pytest.raises(StageClosedError):
        await feed.put(1)
    await asyncio.wait_for(task, TIMEOUT)


async def test_feeder_async_context_manager() -> None:
    got: list[int] = []

    async def handler(n: int) -> None:
        got.append(n)

    pipe = Pipeline("ctx")
    feed = pipe.stage("s", handler).seed_open()
    task = asyncio.create_task(pipe.run())
    await _until(lambda: pipe._ledger is not None)
    async with feed:
        await feed.put(10)
        await _until(lambda: pipe.counts("s").ok == 1)
    await asyncio.wait_for(task, TIMEOUT)
    assert got == [10]
