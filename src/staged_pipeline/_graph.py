"""Граф стадий: спецификация стадии, рёбра, вывод типов, SCC и проверки топологии.

architecture.md §3 (модель), §4.2 (рёбра и маршрутизация), §7.2 (завершение по SCC),
§7.6 (проверки при старте). Граф декларативный и проверяется до запуска: ошибка
топологии бросает `TopologyError`, не запуская ни одного воркера.
"""

import inspect
import types
import typing
from collections.abc import Callable, Iterator, Mapping, Sequence
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Literal, cast

from staged_pipeline._errors import ConfigurationError, TopologyError
from staged_pipeline._policy import Breaker, ExcMap, Retry, validate_policies

__all__ = [
    "Fallback",
    "HandlerInfo",
    "HandlerKind",
    "StageSpec",
    "TopologyWarning",
    "analyze_handler",
    "build_route_map",
    "strongly_connected_components",
    "validate_topology",
]

Fallback = Literal["error", "drop"]
KeyFn = Callable[[object], object]


def _empty_excmap() -> ExcMap:
    """Пустой маппинг исключений (типизированная фабрика для dataclass-поля)."""
    empty: ExcMap = {}
    return empty


def _empty_levels() -> Mapping[str, int]:
    """Пустой маппинг уровней логирования (типизированная фабрика поля)."""
    empty: Mapping[str, int] = {}
    return empty


class HandlerKind(Enum):
    """Вид хендлера, выведенный из сигнатуры."""

    COROUTINE = auto()
    """`async def ...` — возвращает `None`/`Out`/`list`/`tuple`."""
    ASYNC_GEN = auto()
    """`async def ...: yield ...` — потоковый fan-out."""
    SYNC = auto()
    """`def ...` — синхронный хендлер, запускается через `asyncio.to_thread` (executor="thread")."""


@dataclass(frozen=True, slots=True)
class HandlerInfo:
    """Результат разбора сигнатуры хендлера.

    Attributes:
        kind: корутина или асинхронный генератор.
        wants_ctx: есть ли у хендлера второй параметр (`ctx`).
        in_types: конкретные типы входного параметра (развёрнутый Union).
        out_types: конкретные типы, которые хендлер может выдать; пусто —
            терминальная стадия (аннотация выхода `None`).
        streaming: выход потоковый (генератор), а не атомарный (`list`/один).
    """

    kind: HandlerKind
    wants_ctx: bool
    in_types: tuple[type, ...]
    out_types: tuple[type, ...]
    streaming: bool


def _split_union(annotation: object) -> tuple[type, ...]:
    """Развернуть `X | Y` в кортеж типов; `NoneType` отбрасывается."""
    origin = typing.get_origin(annotation)
    # Сравнение значения origin, а не аннотация, — typing.Union здесь уместен.
    if origin is typing.Union or origin is types.UnionType:  # pyright: ignore[reportDeprecated]
        args = cast("tuple[object, ...]", typing.get_args(annotation))
        return tuple(a for a in args if isinstance(a, type) and a is not type(None))
    if isinstance(annotation, type) and annotation is not type(None):
        return (annotation,)
    return ()


_COLLECTION_ORIGINS = frozenset({list, tuple, typing.get_origin(Sequence[object])})


def _element_types(annotation: object, *, streaming: bool) -> tuple[type, ...]:
    """Извлечь типы выдаваемых элементов из аннотации возврата.

    Разворачивает `AsyncIterator[...]`, `list[...]`, `tuple[...]`, затем Union.
    `None`/отсутствие аннотации → пустой кортеж (терминальная стадия).
    """
    if annotation is None or annotation is type(None):
        return ()
    origin = typing.get_origin(annotation)
    if streaming:
        # Возврат async-генератора: AsyncIterator[E] / AsyncGenerator[E, None].
        args = cast("tuple[object, ...]", typing.get_args(annotation))
        return _split_union(args[0]) if args else ()
    if origin in _COLLECTION_ORIGINS:
        args = cast("tuple[object, ...]", typing.get_args(annotation))
        return _split_union(args[0]) if args else ()
    return _split_union(annotation)


def analyze_handler(handler: Callable[..., object], *, executor: str | None = None) -> HandlerInfo:
    """Разобрать сигнатуру хендлера: вид, наличие ctx, типы входа и выхода.

    Args:
        handler: корутина, async-генератор или (при executor='thread') sync-функция;
            `(item)` или `(item, ctx)`.
        executor: 'thread' — хендлер синхронный (запуск через to_thread); иначе async.

    Returns:
        Разбор сигнатуры.

    Raises:
        TopologyError: хендлер не async, без параметров или с неразрешимыми
            аннотациями.
    """
    streaming = inspect.isasyncgenfunction(handler)
    is_async = streaming or inspect.iscoroutinefunction(handler)
    if executor == "thread":
        if is_async:
            raise TopologyError(
                f"хендлер {handler!r} с executor='thread' должен быть синхронным def"
            )
    elif not is_async:
        raise TopologyError(
            f"хендлер {handler!r} должен быть async def или async-генератором "
            + "(для синхронного укажите executor='thread')"
        )

    signature = inspect.signature(handler)
    params = [
        p
        for p in signature.parameters.values()
        if p.kind in (inspect.Parameter.POSITIONAL_ONLY, inspect.Parameter.POSITIONAL_OR_KEYWORD)
    ]
    if not params:
        raise TopologyError(f"хендлер {handler!r} должен принимать элемент первым параметром")
    wants_ctx = len(params) >= 2  # noqa: PLR2004  # (item) или (item, ctx)

    try:
        hints = typing.get_type_hints(handler)
    except (NameError, TypeError) as exc:
        raise TopologyError(f"не удалось разобрать аннотации хендлера {handler!r}: {exc}") from exc

    in_types = _split_union(hints.get(params[0].name))
    if streaming:
        kind = HandlerKind.ASYNC_GEN
    elif is_async:
        kind = HandlerKind.COROUTINE
    else:
        kind = HandlerKind.SYNC
    out_types = _element_types(hints.get("return"), streaming=streaming)
    return HandlerInfo(
        kind=kind,
        wants_ctx=wants_ctx,
        in_types=in_types,
        out_types=out_types,
        streaming=streaming,
    )


@dataclass(slots=True, eq=False)
class StageSpec:
    """Узел графа: конфигурация стадии и её исходящие рёбра.

    Создаётся через `Pipeline.stage(...)` (T2.3). Здесь — модель графа и разбор
    хендлера; рантайм (inbox, воркеры, счётчики) навешивается в `_stage`.
    """

    name: str
    handler: Callable[..., object]
    workers: int = 1
    buffer: int | None = None
    key: KeyFn | None = None
    retry: Retry | None = None
    skip_on: ExcMap = field(default_factory=_empty_excmap)
    error_on: ExcMap = field(default_factory=_empty_excmap)
    abort_if: tuple[Breaker, ...] = ()
    log_levels: Mapping[str, int] = field(default_factory=_empty_levels)
    executor: str | None = None
    seeded: bool = False
    info: HandlerInfo = field(init=False)
    targets: list["StageSpec"] = field(default_factory=list, init=False)
    fallback: Fallback = field(default="error", init=False)

    def __post_init__(self) -> None:
        """Разобрать хендлер и проверить непротиворечивость политик."""
        if self.executor is not None and self.executor != "thread":
            raise ConfigurationError(
                f"executor должен быть None или 'thread', получено {self.executor!r}"
            )
        self.info = analyze_handler(self.handler, executor=self.executor)
        validate_policies(self.retry, self.skip_on, self.error_on)

    def to(self, *targets: "StageSpec", fallback: Fallback = "error") -> None:
        """Объявить рёбра к стадиям-получателям (architecture.md §4.2).

        Args:
            targets: стадии, куда уходит выход.
            fallback: что делать с неотмаршрутизированным выходом — `error`
                (RoutingError, умолчание) или `drop` (счётчик `unrouted`).
        """
        self.targets.extend(targets)
        self.fallback = fallback


def build_route_map(spec: StageSpec) -> dict[type, StageSpec]:
    """Построить карту «тип выхода → стадия» и проверить её однозначность (§4.2).

    Проверка выполняется только при нескольких получателях. Для одного получателя
    весь выход идёт туда без разбора типа.

    Returns:
        Отображение конкретного типа выхода на стадию-получателя (пустое при
        одном получателе).

    Raises:
        TopologyError: тип выхода подходит к нескольким стадиям (неоднозначность)
            или ни к одной (непокрытый член Union).
    """
    if len(spec.targets) <= 1:
        return {}
    route: dict[type, StageSpec] = {}
    for out_type in spec.info.out_types:
        matches = [t for t in spec.targets if _matches_input(out_type, t)]
        if len(matches) > 1:
            names = ", ".join(t.name for t in matches)
            raise TopologyError(
                f"стадия {spec.name!r}: тип выхода {out_type.__name__} подходит к нескольким "
                + f"получателям ({names}); уберите неоднозначность"
            )
        if not matches:
            raise TopologyError(
                f"стадия {spec.name!r}: тип выхода {out_type.__name__} не подходит ни к одному "
                + "получателю; добавьте стадию-получателя для этого типа"
            )
        route[out_type] = matches[0]
    return route


def _matches_input(out_type: type, target: StageSpec) -> bool:
    """Подходит ли тип выхода под вход стадии-получателя (по подтипам)."""
    return any(issubclass(out_type, in_type) for in_type in target.info.in_types)


@dataclass(frozen=True, slots=True)
class TopologyWarning:
    """Необязательное предупреждение проверки топологии (не ошибка)."""

    stage: str
    message: str


def strongly_connected_components(specs: Sequence[StageSpec]) -> list[list[StageSpec]]:  # noqa: C901  # цельный итеративный Тарьян
    """Разбить граф на компоненты сильной связности (итеративный Тарьян).

    Returns:
        Список компонент; каждая — список стадий. Порядок компонент —
        обратный топологический (как у Тарьяна).
    """
    index_of: dict[int, int] = {}
    lowlink: dict[int, int] = {}
    on_stack: set[int] = set()
    stack: list[StageSpec] = []
    result: list[list[StageSpec]] = []
    counter = 0

    for root in specs:
        if id(root) in index_of:
            continue
        # Итеративный DFS: кадр — (узел, итератор по его получателям).
        work: list[tuple[StageSpec, Iterator[StageSpec]]] = [(root, iter(root.targets))]
        index_of[id(root)] = lowlink[id(root)] = counter
        counter += 1
        stack.append(root)
        on_stack.add(id(root))
        while work:
            node, children = work[-1]
            advanced = False
            for child in children:
                if id(child) not in index_of:
                    index_of[id(child)] = lowlink[id(child)] = counter
                    counter += 1
                    stack.append(child)
                    on_stack.add(id(child))
                    work.append((child, iter(child.targets)))
                    advanced = True
                    break
                if id(child) in on_stack:
                    lowlink[id(node)] = min(lowlink[id(node)], index_of[id(child)])
            if advanced:
                continue
            if lowlink[id(node)] == index_of[id(node)]:
                component: list[StageSpec] = []
                while True:
                    member = stack.pop()
                    on_stack.discard(id(member))
                    component.append(member)
                    if member is node:
                        break
                result.append(component)
            work.pop()
            if work:
                parent = work[-1][0]
                lowlink[id(parent)] = min(lowlink[id(parent)], lowlink[id(node)])
    return result


def _in_cycle(specs: Sequence[StageSpec]) -> set[int]:
    """Множество id стадий, лежащих в цикле (компонента >1 или самопетля)."""
    cyclic: set[int] = set()
    for component in strongly_connected_components(specs):
        if len(component) > 1:
            cyclic.update(id(s) for s in component)
        else:
            only = component[0]
            if only in only.targets:
                cyclic.add(id(only))
    return cyclic


def validate_topology(specs: Sequence[StageSpec]) -> list[TopologyWarning]:
    """Проверить граф перед запуском (architecture.md §7.6).

    Жёсткие нарушения бросают `TopologyError`; «мягкие» (стадия без входа)
    возвращаются как предупреждения.

    Returns:
        Список предупреждений (стадии без входа и без seed).

    Raises:
        TopologyError: bounded-буфер в цикле; терминальная стадия с выходом не
            `None`; неоднозначный/непокрытый маршрут; retry у генератора при
            получателях без `key`.
    """
    cyclic = _in_cycle(specs)
    has_incoming: set[int] = set()
    for spec in specs:
        for target in spec.targets:
            has_incoming.add(id(target))

    warnings: list[TopologyWarning] = []
    for spec in specs:
        if spec.buffer is not None and id(spec) in cyclic:
            raise TopologyError(
                f"стадия {spec.name!r} в цикле не может иметь bounded-буфер "
                + f"(buffer={spec.buffer}); источник deadlock, уберите ограничение"
            )
        if not spec.targets and spec.info.out_types:
            kinds = ", ".join(t.__name__ for t in spec.info.out_types)
            raise TopologyError(
                f"терминальная стадия {spec.name!r} объявляет выход ({kinds}), но не имеет .to(); "
                + "укажите получателя или верните None"
            )
        build_route_map(spec)  # бросит при неоднозначном/непокрытом маршруте
        if spec.retry is not None and spec.info.streaming:
            bad = [t.name for t in spec.targets if t.key is None]
            if bad:
                names = ", ".join(bad)
                raise TopologyError(
                    f"стадия {spec.name!r} с retry и генератором требует key у получателей, "
                    + f"иначе повтор задвоит элементы; без key: {names}"
                )
        if id(spec) not in has_incoming and not spec.seeded:
            warnings.append(
                TopologyWarning(spec.name, "нет входящих рёбер и seed — закроется сразу")
            )
    return warnings
