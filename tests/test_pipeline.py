"""Тесты движка: воркер, супервизор, Pipeline.run (T2.3)."""

import asyncio
from collections.abc import AsyncIterator

import pytest

from staged_pipeline._pipeline import Pipeline
from staged_pipeline._stage import StageContext

TIMEOUT = 3.0


async def test_dag_three_stages_completes() -> None:
    collected: list[int] = []

    async def produce(n: int) -> list[int]:
        return [n * 10, n * 10 + 1]

    async def transform(x: int) -> int:
        return x + 1

    async def collect(x: int) -> None:
        collected.append(x)

    pipe = Pipeline("dag")
    s1 = pipe.stage("produce", produce)
    s2 = pipe.stage("transform", transform)
    s3 = pipe.stage("collect", collect)
    s1.to(s2)
    s2.to(s3)
    s1.seed([1, 2, 3])

    await asyncio.wait_for(pipe.run(), TIMEOUT)

    assert pipe.counts("produce").ok == 3
    assert pipe.counts("transform").ok == 6
    assert pipe.counts("collect").ok == 6
    assert len(collected) == 6


async def test_crawler_self_loop_completes() -> None:
    visited: list[int] = []

    async def crawl(depth: int) -> list[int]:
        visited.append(depth)
        if depth < 3:
            return [depth + 1, depth + 1]
        return []

    pipe = Pipeline("crawl")
    stage = pipe.stage("crawl", crawl)
    stage.to(stage)
    stage.seed([0])

    await asyncio.wait_for(pipe.run(), TIMEOUT)

    # 1 (d0) + 2 (d1) + 4 (d2) + 8 (d3) = 15 узлов.
    assert pipe.counts("crawl").ok == 15
    assert len(visited) == 15


async def test_last_item_children_not_dropped() -> None:
    out: list[int] = []

    async def src(n: int) -> list[int]:
        return [n, n]

    async def sink(x: int) -> None:
        out.append(x)

    pipe = Pipeline("fanout")
    a = pipe.stage("src", src)
    b = pipe.stage("sink", sink)
    a.to(b)
    a.seed([1, 2, 3])

    await asyncio.wait_for(pipe.run(), TIMEOUT)
    assert sorted(out) == [1, 1, 2, 2, 3, 3]


async def test_failure_unblocks_upstream_on_bounded_put() -> None:
    async def src(n: int) -> int:
        return n

    async def boom(x: int) -> None:  # noqa: ARG001
        raise ValueError("boom")

    pipe = Pipeline("fail")
    a = pipe.stage("src", src, workers=1)
    b = pipe.stage("boom", boom, workers=1, buffer=1)
    a.to(b)
    a.seed(list(range(100)))

    with pytest.raises(ValueError, match="boom"):
        await asyncio.wait_for(pipe.run(), TIMEOUT)


async def test_generator_closed_on_failure() -> None:
    closed: list[str] = []

    async def source(_: int) -> AsyncIterator[int]:
        try:
            value = 0
            while True:
                yield value
                value += 1
        finally:
            closed.append("gen")

    async def boom(x: int) -> None:  # noqa: ARG001
        raise ValueError("stop")

    pipe = Pipeline("genfail")
    a = pipe.stage("gen", source)
    b = pipe.stage("boom", boom, buffer=1)
    a.to(b)
    a.seed([0])

    with pytest.raises(ValueError, match="stop"):
        await asyncio.wait_for(pipe.run(), TIMEOUT)
    assert closed == ["gen"]


async def test_second_run_rejected() -> None:
    async def handler(_: int) -> None:
        return None

    pipe = Pipeline("once")
    stage = pipe.stage("s", handler)
    stage.seed([1])
    await asyncio.wait_for(pipe.run(), TIMEOUT)

    with pytest.raises(RuntimeError, match="одноразов"):
        await pipe.run()


async def test_duplicate_stage_name_rejected() -> None:
    async def handler(_: int) -> None:
        return None

    pipe = Pipeline("dup")
    pipe.stage("s", handler)
    with pytest.raises(Exception, match="уже объявлена"):
        pipe.stage("s", handler)


async def test_topology_checked_before_run() -> None:
    async def crawl(n: int) -> int:
        return n

    pipe = Pipeline("bad")
    stage = pipe.stage("crawl", crawl, buffer=5)  # bounded в цикле
    stage.to(stage)
    stage.seed([1])

    with pytest.raises(Exception, match="цикл"):
        await pipe.run()


async def test_stage_without_input_closes_immediately() -> None:
    ran: list[int] = []

    async def source(n: int) -> None:
        ran.append(n)

    async def orphan(n: int) -> None:
        ran.append(n)

    pipe = Pipeline("orphan")
    pipe.stage("source", source).seed([1])
    pipe.stage("orphan", orphan)  # ни рёбер, ни seed → закроется сразу

    await asyncio.wait_for(pipe.run(), TIMEOUT)
    assert ran == [1]  # orphan не получил ничего и не завис


async def test_ctx_label_and_incr() -> None:
    async def work(n: int, ctx: StageContext) -> None:
        ctx.label("done")
        ctx.incr("bytes", n)

    pipe = Pipeline("ctx")
    pipe.stage("work", work).seed([10, 20])
    await asyncio.wait_for(pipe.run(), TIMEOUT)

    counts = pipe.counts("work")
    assert counts.labels["done"] == 2
    assert counts.metrics["bytes"] == 30
