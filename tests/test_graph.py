"""Тесты графа: разбор хендлеров, SCC, проверки топологии, маршрутизация (T2.1)."""

from collections.abc import AsyncIterator, Callable

import pytest

from staged_pipeline._errors import TopologyError
from staged_pipeline._graph import (
    HandlerKind,
    StageSpec,
    analyze_handler,
    build_route_map,
    strongly_connected_components,
    validate_topology,
)
from staged_pipeline._policy import Retry


# --- доменные типы и хендлеры для тестов ---------------------------------- #
class Folder:
    pass


class Ref:
    pass


class Spec:
    pass


class Page:
    pass


class Image:
    pass


async def scan(folder: Folder) -> AsyncIterator[Ref]:  # noqa: ARG001
    yield Ref()


async def resolve(ref: Ref) -> list[Spec]:  # noqa: ARG001
    return [Spec()]


async def download(spec: Spec) -> None:  # noqa: ARG001
    return None


async def with_ctx(ref: Ref, ctx: object) -> None:  # noqa: ARG001
    return None


async def split(page: Page) -> Page | Image:
    return page


async def passthrough_int(x: int) -> int:
    return x


def _spec(
    name: str,
    handler: Callable[..., object] = passthrough_int,
    *,
    buffer: int | None = None,
    key: Callable[[object], object] | None = None,
    retry: Retry | None = None,
    seeded: bool = False,
) -> StageSpec:
    return StageSpec(name=name, handler=handler, buffer=buffer, key=key, retry=retry, seeded=seeded)


# --- разбор хендлеров ------------------------------------------------------ #
def test_analyze_coroutine_single_output() -> None:
    info = analyze_handler(split)
    assert info.kind is HandlerKind.COROUTINE
    assert not info.streaming
    assert info.in_types == (Page,)
    assert set(info.out_types) == {Page, Image}
    assert not info.wants_ctx


def test_analyze_async_generator() -> None:
    info = analyze_handler(scan)
    assert info.kind is HandlerKind.ASYNC_GEN
    assert info.streaming
    assert info.out_types == (Ref,)


def test_analyze_list_fanout() -> None:
    info = analyze_handler(resolve)
    assert info.out_types == (Spec,)
    assert not info.streaming


def test_analyze_terminal_none_output() -> None:
    info = analyze_handler(download)
    assert info.out_types == ()


def test_analyze_wants_ctx() -> None:
    assert analyze_handler(with_ctx).wants_ctx
    assert not analyze_handler(download).wants_ctx


def test_analyze_rejects_sync_handler() -> None:
    def sync_handler(x: int) -> int:
        return x

    with pytest.raises(TopologyError, match="async"):
        analyze_handler(sync_handler)


def test_analyze_rejects_no_params() -> None:
    async def no_params() -> None:
        return None

    with pytest.raises(TopologyError, match="перв"):
        analyze_handler(no_params)


# --- SCC ------------------------------------------------------------------- #
def test_scc_dag_all_singletons() -> None:
    a, b, c = _spec("a"), _spec("b"), _spec("c")
    a.to(b)
    b.to(c)
    components = strongly_connected_components([a, b, c])
    assert sorted(len(comp) for comp in components) == [1, 1, 1]


def test_scc_self_loop() -> None:
    crawl = _spec("crawl")
    crawl.to(crawl)
    components = strongly_connected_components([crawl])
    assert len(components) == 1
    assert components[0] == [crawl]


def test_scc_two_stage_cycle() -> None:
    a, b = _spec("a"), _spec("b")
    a.to(b)
    b.to(a)
    components = strongly_connected_components([a, b])
    cycle = next(comp for comp in components if len(comp) == 2)
    assert {s.name for s in cycle} == {"a", "b"}


# --- проверки §7.6 --------------------------------------------------------- #
def test_bounded_buffer_in_cycle_rejected() -> None:
    crawl = _spec("crawl", buffer=10)
    crawl.to(crawl)
    with pytest.raises(TopologyError, match="цикл"):
        validate_topology([crawl])


def test_terminal_stage_with_output_rejected() -> None:
    # resolve выдаёт Spec, но не имеет .to() → терминальная стадия с выходом.
    stage = _spec("resolve", handler=resolve)
    with pytest.raises(TopologyError, match="терминальная"):
        validate_topology([stage])


def test_ambiguous_route_rejected() -> None:
    source = _spec("split", handler=split)
    specific = _spec("pages", handler=download_page)
    catch_all = _spec("any", handler=download_any)
    source.to(specific, catch_all)
    with pytest.raises(TopologyError, match="нескольким"):
        validate_topology([source, specific, catch_all])


def test_uncovered_union_member_rejected() -> None:
    source = _spec("split", handler=split)
    pages = _spec("pages", handler=download_page)
    others = _spec("specs", handler=download)
    source.to(pages, others)
    with pytest.raises(TopologyError, match="не подходит"):
        validate_topology([source, pages, others])


def test_retry_generator_requires_key_on_receivers() -> None:
    source = _spec("scan", handler=scan, retry=Retry(on=ValueError, attempts=2))
    sink = _spec("resolve", handler=resolve_ref)
    source.to(sink)
    with pytest.raises(TopologyError, match="key"):
        validate_topology([source, sink])


def test_retry_generator_with_keyed_receiver_ok() -> None:
    source = _spec("scan", handler=scan, retry=Retry(on=ValueError, attempts=2))
    sink = _spec("resolve", handler=resolve_ref, key=id)
    source.to(sink)
    validate_topology([source, sink])  # не бросает


def test_stage_without_input_warns() -> None:
    source = _spec("scan", handler=scan)
    sink = _spec("resolve", handler=resolve_ref, key=id)
    source.to(sink)
    warnings = validate_topology([source, sink])
    assert any(w.stage == "scan" for w in warnings)


def test_seeded_source_no_warning() -> None:
    source = _spec("scan", handler=scan, seeded=True)
    sink = _spec("resolve", handler=resolve_ref, key=id)
    source.to(sink)
    warnings = validate_topology([source, sink])
    assert not any(w.stage == "scan" for w in warnings)


# --- маршрутизация --------------------------------------------------------- #
def test_build_route_map_splits_union() -> None:
    source = _spec("split", handler=split)
    pages = _spec("pages", handler=download_page)
    images = _spec("images", handler=download_image)
    source.to(pages, images)
    route = build_route_map(source)
    assert route == {Page: pages, Image: images}


def test_single_target_needs_no_route_map() -> None:
    source = _spec("resolve", handler=resolve)
    sink = _spec("media", handler=download)
    source.to(sink)
    assert build_route_map(source) == {}


# вспомогательные хендлеры для маршрутизации
async def download_page(page: Page) -> None:  # noqa: ARG001
    return None


async def download_image(image: Image) -> None:  # noqa: ARG001
    return None


async def download_any(obj: object) -> None:  # noqa: ARG001
    return None


async def resolve_ref(ref: Ref) -> None:  # noqa: ARG001  # терминальный приёмник
    return None
