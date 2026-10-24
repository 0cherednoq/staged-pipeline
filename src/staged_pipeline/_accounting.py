"""Учёт элементов и завершение компонент (architecture.md §3.1, §5, §7.2).

Счётчики стадии меняются в одной точке — функции перехода `Ledger.transition`
(§5, правило 5). Компонента (сильно связная часть графа) закрывается ровно когда
`pending == 0 and writers == 0`; закрытие каскадно уменьшает `writers` у
нижележащих компонент (§7.2). Инвариант на любой момент:
`found == queued + in_flight + deferred + done`.
"""

from collections import Counter
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from enum import Enum, auto

from staged_pipeline._graph import StageSpec, strongly_connected_components

__all__ = ["Event", "Ledger", "StageCounts"]


class Event(Enum):
    """Переход в жизненном цикле элемента (architecture.md §3.1)."""

    ACCEPT = auto()
    """Предложен и принят: ключ → seen. `found+1`, `queued+1`."""
    DUPLICATE = auto()
    """Предложен, но ключ уже принят. `duplicates+1`."""
    START = auto()
    """Воркер взял из очереди. `queued-1`, `in_flight+1`."""
    OK = auto()
    """Хендлер вернулся. `in_flight-1`, `ok+1`."""
    SKIP = auto()
    """Исход skip. `in_flight-1`, `skip+1`."""
    ERROR = auto()
    """Исход error. `in_flight-1`, `error+1`."""
    DEFER = auto()
    """Ретрай отложен. `in_flight-1`, `deferred+1`."""
    REDELIVER = auto()
    """Наступил not_before. `deferred-1`, `queued+1`."""
    CANCEL_QUEUED = auto()
    """Отменён из очереди. `queued-1`, `cancelled+1`."""
    CANCEL_DEFERRED = auto()
    """Отменён из отложенных. `deferred-1`, `cancelled+1`."""


@dataclass(slots=True)
class StageCounts:
    """Счётчики одной стадии (словарь прогресса, architecture.md §4.8)."""

    found: int = 0
    queued: int = 0
    in_flight: int = 0
    deferred: int = 0
    ok: int = 0
    skip: int = 0
    error: int = 0
    cancelled: int = 0
    duplicates: int = 0
    labels: Counter[str] = field(default_factory=Counter)
    metrics: Counter[str] = field(default_factory=Counter)
    emitted: Counter[str] = field(default_factory=Counter)

    @property
    def done(self) -> int:
        """Завершённые исходы: `ok + skip + error + cancelled`."""
        return self.ok + self.skip + self.error + self.cancelled

    @property
    def in_pipeline(self) -> int:
        """Элементы «в пути»: `queued + in_flight + deferred`."""
        return self.queued + self.in_flight + self.deferred

    def invariant_holds(self) -> bool:
        """Проверить инвариант `found == queued + in_flight + deferred + done`."""
        return self.found == self.in_pipeline + self.done


@dataclass(slots=True)
class _Component:
    """Компонента сильной связности: единица закрытия (architecture.md §7.2)."""

    index: int
    members: frozenset[str]
    downstream: dict[int, int]  # индекс компоненты-получателя → число рёбер
    pending: int = 0
    writers: int = 0
    closed: bool = False


class Ledger:
    """Счётчики всех стадий и завершение компонент.

    Создаётся из спецификаций графа: вычисляет компоненты (SCC), начальные
    `writers` (входящие межкомпонентные рёбра + открытые seed-входы) и ведёт
    `pending`. Закрытие компоненты вызывает `on_close` и каскадно уменьшает
    `writers` у получателей.
    """

    def __init__(
        self,
        specs: Sequence[StageSpec],
        *,
        seed_inputs: Mapping[str, int] | None = None,
        on_close: Callable[[frozenset[str]], None] | None = None,
    ) -> None:
        """Построить реестр.

        Args:
            specs: стадии пайплайна.
            seed_inputs: число открытых seed-входов на стадию (по умолчанию 1 для
                стадий с `seeded=True`, иначе 0). Каждый вход — один `writer`.
            on_close: колбэк при закрытии компоненты (получает имена её стадий).
        """
        self._on_close: Callable[[frozenset[str]], None] | None = on_close
        self._counts: dict[str, StageCounts] = {spec.name: StageCounts() for spec in specs}
        self._comp_of: dict[str, int] = {}
        self._components: list[_Component] = []
        self._build_components(specs)
        self._init_writers(specs, seed_inputs or {})

    def _build_components(self, specs: Sequence[StageSpec]) -> None:
        """Разбить граф на компоненты и запомнить принадлежность стадий."""
        for index, component in enumerate(strongly_connected_components(specs)):
            members = frozenset(spec.name for spec in component)
            for name in members:
                self._comp_of[name] = index
            self._components.append(_Component(index=index, members=members, downstream={}))

    def _init_writers(self, specs: Sequence[StageSpec], seed_inputs: Mapping[str, int]) -> None:
        """Посчитать межкомпонентные рёбра (downstream и writers) и seed-входы."""
        for spec in specs:
            src = self._comp_of[spec.name]
            for target in spec.targets:
                dst = self._comp_of[target.name]
                if dst == src:
                    continue  # внутрикомпонентное ребро — не writer
                self._components[src].downstream[dst] = (
                    self._components[src].downstream.get(dst, 0) + 1
                )
                self._components[dst].writers += 1
        for spec in specs:
            opened = seed_inputs.get(spec.name, 1 if spec.seeded else 0)
            self._components[self._comp_of[spec.name]].writers += opened

    # --- доступ -------------------------------------------------------------
    def counts(self, stage: str) -> StageCounts:
        """Счётчики стадии по имени."""
        return self._counts[stage]

    def is_closed(self, stage: str) -> bool:
        """Закрыта ли компонента, в которой лежит стадия."""
        return self._components[self._comp_of[stage]].closed

    def invariant_holds(self) -> bool:
        """Инвариант счётчиков выполняется для всех стадий."""
        return all(counts.invariant_holds() for counts in self._counts.values())

    # --- переходы -----------------------------------------------------------
    def transition(
        self,
        stage: str,
        event: Event,
        *,
        label: str | None = None,
        count: int = 1,
    ) -> None:
        """Применить переход к стадии — единственная точка смены счётчиков (§5).

        Args:
            stage: имя стадии.
            event: вид перехода.
            label: метка исхода для OK/SKIP/ERROR (по умолчанию имя исхода).
            count: кратность (для массовой отмены CANCEL_*).
        """
        counts = self._counts[stage]
        comp = self._components[self._comp_of[stage]]
        match event:
            case Event.ACCEPT:
                counts.found += 1
                counts.queued += 1
                comp.pending += 1
            case Event.DUPLICATE:
                counts.duplicates += 1
            case Event.START:
                counts.queued -= 1
                counts.in_flight += 1
            case Event.OK | Event.SKIP | Event.ERROR:
                self._finish(stage, event, label)
            case Event.DEFER:
                counts.in_flight -= 1
                counts.deferred += 1
            case Event.REDELIVER:
                counts.deferred -= 1
                counts.queued += 1
            case Event.CANCEL_QUEUED:
                counts.queued -= count
                counts.cancelled += count
                comp.pending -= count
                self._check_close(comp)
            case Event.CANCEL_DEFERRED:
                counts.deferred -= count
                counts.cancelled += count
                comp.pending -= count
                self._check_close(comp)

    def _finish(self, stage: str, event: Event, label: str | None) -> None:
        """Завершить обработку элемента (OK/SKIP/ERROR): минус in_flight, плюс исход."""
        counts = self._counts[stage]
        comp = self._components[self._comp_of[stage]]
        counts.in_flight -= 1
        if event is Event.OK:
            counts.ok += 1
            counts.labels[label or "ok"] += 1
        elif event is Event.SKIP:
            counts.skip += 1
            counts.labels[label or "skip"] += 1
        else:
            counts.error += 1
            counts.labels[label or "error"] += 1
        comp.pending -= 1
        self._check_close(comp)

    def emit(self, stage: str, target: str) -> None:
        """Учесть выход стадии по ребру (для fan-out ratio, §4.8)."""
        self._counts[stage].emitted[target] += 1

    def incr(self, stage: str, metric: str, value: int = 1) -> None:
        """Увеличить доменную метрику стадии (`ctx.incr`)."""
        self._counts[stage].metrics[metric] += value

    def check_all(self) -> None:
        """Прогнать проверку закрытия по всем компонентам (вызов при старте run).

        Закрывает компоненты, которые уже при старте удовлетворяют условию
        (например, стадия без входа и без seed: writers=0, pending=0).
        """
        for comp in list(self._components):
            self._check_close(comp)

    def seal_seed(self, stage: str) -> None:
        """Закрыть один открытый seed-вход стадии (writers компоненты -1)."""
        comp = self._components[self._comp_of[stage]]
        comp.writers -= 1
        self._check_close(comp)

    # --- закрытие -----------------------------------------------------------
    def _check_close(self, comp: _Component) -> None:
        """Закрыть компоненту, если pending и writers обнулились; каскад вниз."""
        if comp.closed or comp.pending != 0 or comp.writers != 0:
            return
        comp.closed = True
        if self._on_close is not None:
            self._on_close(comp.members)
        for dst_index, edges in comp.downstream.items():
            dst = self._components[dst_index]
            dst.writers -= edges
            self._check_close(dst)
