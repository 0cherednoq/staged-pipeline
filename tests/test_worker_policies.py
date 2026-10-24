"""Тесты политик в воркере: ретрай, skip/error, abort_if, логирование (T2.4)."""

import asyncio
import logging
from collections.abc import Callable

import pytest

from staged_pipeline._clock import FakeClock
from staged_pipeline._errors import Error, Skip, StageAborted
from staged_pipeline._pipeline import Pipeline
from staged_pipeline._policy import ErrorRate, Fixed, NoSuccess, Retry

TIMEOUT = 3.0


class NetError(Exception):
    pass


class NotFound(Exception):
    pass


async def _until(predicate: Callable[[], bool]) -> None:
    for _ in range(1000):
        await asyncio.sleep(0)  # сначала дать run() стартовать (создать ledger)
        if predicate():
            return
    msg = "условие не выполнилось"
    raise AssertionError(msg)


async def test_retry_does_not_hold_worker() -> None:
    clock = FakeClock()
    fails = {1: 1}  # элемент 1 падает один раз
    seen: list[int] = []

    async def handler(n: int) -> None:
        seen.append(n)
        if fails.get(n, 0) > 0:
            fails[n] -= 1
            raise NetError

    pipe = Pipeline("retry", clock=clock)
    pipe.stage(
        "s",
        handler,
        workers=1,
        retry=Retry(on=NetError, attempts=3, backoff=Fixed(5.0)),
    ).seed([1, 2])

    task = asyncio.create_task(pipe.run())
    # Элемент 1 упал и отложен; элемент 2 обработан, пока 1 ждёт backoff.
    await _until(lambda: seen == [1, 2])
    await _until(lambda: bool(clock._sleepers))  # воркер уснул до дозревания retry
    assert pipe.counts("s").deferred == 1
    assert pipe.counts("s").ok == 1

    clock.advance(5)
    await asyncio.wait_for(task, TIMEOUT)
    assert seen == [1, 2, 1]
    assert pipe.counts("s").ok == 2
    assert pipe.counts("s").deferred == 0


async def test_skip_on_maps_to_skip() -> None:
    async def handler(n: int) -> None:  # noqa: ARG001
        raise NotFound

    pipe = Pipeline("skip")
    pipe.stage("s", handler, skip_on={NotFound: "deleted"}).seed([1, 2])
    await asyncio.wait_for(pipe.run(), TIMEOUT)
    counts = pipe.counts("s")
    assert counts.skip == 2
    assert counts.labels["deleted"] == 2
    assert counts.ok == 0


async def test_error_on_maps_to_error() -> None:
    async def handler(n: int) -> None:  # noqa: ARG001
        raise NetError

    pipe = Pipeline("err")
    pipe.stage("s", handler, error_on={NetError: "net_failed"}).seed([1, 2])
    await asyncio.wait_for(pipe.run(), TIMEOUT)
    counts = pipe.counts("s")
    assert counts.error == 2
    assert counts.labels["net_failed"] == 2


async def test_raise_skip_and_error_directly() -> None:
    async def handler(n: int) -> None:
        if n == 1:
            raise Skip("one")
        raise Error("two")

    pipe = Pipeline("direct")
    pipe.stage("s", handler).seed([1, 2])
    await asyncio.wait_for(pipe.run(), TIMEOUT)
    counts = pipe.counts("s")
    assert counts.skip == 1
    assert counts.error == 1
    assert counts.labels["one"] == 1
    assert counts.labels["two"] == 1


async def test_retry_exhausted_falls_to_error_on() -> None:
    clock = FakeClock()

    async def handler(n: int) -> None:  # noqa: ARG001
        raise NetError

    pipe = Pipeline("exhaust", clock=clock)
    pipe.stage(
        "s",
        handler,
        workers=1,
        retry=Retry(on=NetError, attempts=2, backoff=Fixed(1.0)),
        error_on={Exception: "gave_up"},  # широкий класс: NetError ретраится, затем сюда
    ).seed([1])

    task = asyncio.create_task(pipe.run())
    await _until(lambda: pipe.counts("s").deferred == 1)
    await _until(lambda: bool(clock._sleepers))  # воркер уснул до дозревания retry
    clock.advance(1)
    await asyncio.wait_for(task, TIMEOUT)
    counts = pipe.counts("s")
    assert counts.error == 1
    assert counts.labels["gave_up"] == 1


async def test_no_success_aborts_at_close() -> None:
    async def handler(n: int) -> None:  # noqa: ARG001
        raise NetError("boom")

    pipe = Pipeline("nosuccess")
    pipe.stage(
        "s",
        handler,
        error_on={NetError: "failed"},
        abort_if=NoSuccess(),
    ).seed([1, 2, 3])

    with pytest.raises(NetError, match="boom"):
        await asyncio.wait_for(pipe.run(), TIMEOUT)


async def test_error_rate_aborts_during_work() -> None:
    async def handler(n: int) -> None:  # noqa: ARG001
        raise NetError("rate")

    pipe = Pipeline("rate")
    pipe.stage(
        "s",
        handler,
        workers=1,
        error_on={NetError: "failed"},
        abort_if=ErrorRate(1.0, min_samples=20),
    ).seed(list(range(40)))

    with pytest.raises(NetError, match="rate"):
        await asyncio.wait_for(pipe.run(), TIMEOUT)
    # Сработал до обработки всех 40.
    assert pipe.counts("s").error < 40


async def test_abort_without_source_exception_is_stage_aborted() -> None:
    async def handler(n: int) -> None:  # noqa: ARG001
        raise Error("domain")

    pipe = Pipeline("abort")
    pipe.stage("s", handler, abort_if=NoSuccess()).seed([1, 2])
    with pytest.raises(StageAborted, match="NoSuccess"):
        await asyncio.wait_for(pipe.run(), TIMEOUT)


async def test_skip_is_logged(caplog: pytest.LogCaptureFixture) -> None:
    async def handler(n: int) -> None:  # noqa: ARG001
        raise NotFound

    pipe = Pipeline("log")
    pipe.stage("resolve", handler, skip_on={NotFound: "deleted"}).seed([1])
    with caplog.at_level(logging.WARNING, logger="staged_pipeline"):
        await asyncio.wait_for(pipe.run(), TIMEOUT)
    assert any("resolve" in r.message and "NotFound" in r.message for r in caplog.records)
