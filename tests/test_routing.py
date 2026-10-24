"""Тесты маршрутизации по типу и executor='thread' (T3.2)."""

import asyncio
import threading
from typing import cast

import pytest

from staged_pipeline._errors import ConfigurationError, RoutingError, TopologyError
from staged_pipeline._pipeline import Pipeline

TIMEOUT = 3.0


class Page:
    pass


class Image:
    pass


async def test_routing_by_type_splits_union() -> None:
    pages: list[Page] = []
    images: list[Image] = []

    async def source(n: int) -> Page | Image:
        return Page() if n % 2 == 0 else Image()

    async def on_page(p: Page) -> None:
        pages.append(p)

    async def on_image(i: Image) -> None:
        images.append(i)

    pipe = Pipeline("route")
    src = pipe.stage("src", source)
    pg = pipe.stage("pages", on_page)
    im = pipe.stage("images", on_image)
    src.to(pg, im)
    src.seed([0, 1, 2, 3])

    await asyncio.wait_for(pipe.run(), TIMEOUT)
    assert len(pages) == 2
    assert len(images) == 2
    assert pipe.counts("src").emitted["pages"] == 2
    assert pipe.counts("src").emitted["images"] == 2


async def test_routing_miss_raises_routing_error() -> None:
    async def source(n: int) -> Page | Image:  # noqa: ARG001
        return cast("Page", object())  # лжёт аннотации: ни Page, ни Image

    async def on_page(p: Page) -> None:  # noqa: ARG001
        return None

    async def on_image(i: Image) -> None:  # noqa: ARG001
        return None

    pipe = Pipeline("miss")
    src = pipe.stage("src", source)
    src.to(pipe.stage("pages", on_page), pipe.stage("images", on_image))
    src.seed([1])

    with pytest.raises(RoutingError, match="не подошёл"):
        await asyncio.wait_for(pipe.run(), TIMEOUT)


async def test_routing_miss_drop_counts_unrouted() -> None:
    async def source(n: int) -> Page | Image:  # noqa: ARG001
        return cast("Page", object())

    async def on_page(p: Page) -> None:  # noqa: ARG001
        return None

    async def on_image(i: Image) -> None:  # noqa: ARG001
        return None

    pipe = Pipeline("drop")
    src = pipe.stage("src", source)
    src.to(pipe.stage("pages", on_page), pipe.stage("images", on_image), fallback="drop")
    src.seed([1, 2])

    await asyncio.wait_for(pipe.run(), TIMEOUT)
    assert pipe.counts("src").metrics["unrouted"] == 2


async def test_executor_thread_runs_sync_handler_off_loop() -> None:
    thread_ids: list[int] = []
    results: list[int] = []

    def work(n: int) -> int:
        thread_ids.append(threading.get_ident())
        return n * 2

    async def collect(n: int) -> None:
        results.append(n)

    pipe = Pipeline("thread")
    w = pipe.stage("work", work, executor="thread")
    w.to(pipe.stage("collect", collect))
    w.seed([1, 2, 3])

    await asyncio.wait_for(pipe.run(), TIMEOUT)
    assert sorted(results) == [2, 4, 6]
    main_id = threading.main_thread().ident
    assert all(tid != main_id for tid in thread_ids)


def test_sync_handler_without_executor_rejected() -> None:
    def work(n: int) -> int:
        return n

    pipe = Pipeline("bad")
    with pytest.raises(TopologyError, match="executor"):
        pipe.stage("w", work)


def test_async_handler_with_thread_executor_rejected() -> None:
    async def work(n: int) -> int:
        return n

    pipe = Pipeline("bad2")
    with pytest.raises(TopologyError, match="синхронным"):
        pipe.stage("w", work, executor="thread")


def test_invalid_executor_rejected() -> None:
    def work(n: int) -> int:
        return n

    pipe = Pipeline("bad3")
    with pytest.raises(ConfigurationError, match="executor"):
        pipe.stage("w", work, executor="process")
