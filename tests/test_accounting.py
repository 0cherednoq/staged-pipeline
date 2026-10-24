"""Тесты учёта и завершения компонент (T2.2): переходы, каскад, hypothesis-инвариант."""

from hypothesis import strategies as st
from hypothesis.stateful import RuleBasedStateMachine, invariant, rule

from staged_pipeline._accounting import Event, Ledger, StageCounts
from staged_pipeline._graph import StageSpec


class A:
    pass


async def h_src(x: int) -> A:  # noqa: ARG001
    return A()


async def h_mid(a: A) -> A:
    return a


def _make_specs() -> list[StageSpec]:
    """Граф из 3 стадий с циклом: s0 → s1 ⇄ s2 (s0 — засеянный источник)."""
    s0 = StageSpec(name="s0", handler=h_src, seeded=True)
    s1 = StageSpec(name="s1", handler=h_mid)
    s2 = StageSpec(name="s2", handler=h_mid)
    s0.to(s1)
    s1.to(s2)
    s2.to(s1)
    return [s0, s1, s2]


# --- простые переходы ------------------------------------------------------ #
def test_accept_updates_found_and_pending() -> None:
    ledger = Ledger(_make_specs())
    ledger.transition("s1", Event.ACCEPT)
    counts = ledger.counts("s1")
    assert counts.found == 1
    assert counts.queued == 1
    assert counts.invariant_holds()


def test_full_ok_lifecycle() -> None:
    ledger = Ledger(_make_specs())
    ledger.transition("s1", Event.ACCEPT)
    ledger.transition("s1", Event.START)
    ledger.transition("s1", Event.OK, label="saved")
    counts = ledger.counts("s1")
    assert (counts.queued, counts.in_flight, counts.ok) == (0, 0, 1)
    assert counts.labels["saved"] == 1
    assert counts.invariant_holds()


def test_defer_then_redeliver() -> None:
    ledger = Ledger(_make_specs())
    ledger.transition("s1", Event.ACCEPT)
    ledger.transition("s1", Event.START)
    ledger.transition("s1", Event.DEFER)
    assert ledger.counts("s1").deferred == 1
    ledger.transition("s1", Event.REDELIVER)
    assert ledger.counts("s1").deferred == 0
    assert ledger.counts("s1").queued == 1
    assert ledger.counts("s1").invariant_holds()


def test_duplicate_not_counted_in_found() -> None:
    ledger = Ledger(_make_specs())
    ledger.transition("s1", Event.DUPLICATE)
    counts = ledger.counts("s1")
    assert counts.duplicates == 1
    assert counts.found == 0
    assert counts.invariant_holds()


def test_cancel_queued_and_deferred() -> None:
    ledger = Ledger(_make_specs())
    for _ in range(3):
        ledger.transition("s1", Event.ACCEPT)
    ledger.transition("s1", Event.CANCEL_QUEUED, count=2)
    ledger.transition("s1", Event.START)
    ledger.transition("s1", Event.DEFER)
    ledger.transition("s1", Event.CANCEL_DEFERRED)
    counts = ledger.counts("s1")
    assert counts.cancelled == 3
    assert counts.invariant_holds()


def test_emit_and_incr() -> None:
    ledger = Ledger(_make_specs())
    ledger.emit("s0", "s1")
    ledger.emit("s0", "s1")
    ledger.incr("s0", "bytes", 512)
    assert ledger.counts("s0").emitted["s1"] == 2
    assert ledger.counts("s0").metrics["bytes"] == 512


# --- закрытие компонент ---------------------------------------------------- #
def test_source_closes_after_seal_and_drain() -> None:
    closed: list[frozenset[str]] = []
    ledger = Ledger(_make_specs(), on_close=closed.append)
    # Источник s0: один элемент прошёл, затем seed запечатан.
    ledger.transition("s0", Event.ACCEPT)
    ledger.transition("s0", Event.START)
    ledger.transition("s0", Event.OK)
    assert not ledger.is_closed("s0")  # seed ещё открыт (writer)
    ledger.seal_seed("s0")
    assert ledger.is_closed("s0")
    assert frozenset({"s0"}) in closed


def test_closing_cascades_to_downstream_component() -> None:
    closed: list[frozenset[str]] = []
    ledger = Ledger(_make_specs(), on_close=closed.append)
    # Закрываем источник: его закрытие снимает writer у компоненты {s1, s2}.
    ledger.seal_seed("s0")
    assert ledger.is_closed("s0")
    # Компонента цикла ещё держится? writers стало 0, pending 0 → тоже закрылась.
    assert ledger.is_closed("s1")
    assert ledger.is_closed("s2")
    assert any(members == frozenset({"s1", "s2"}) for members in closed)


def test_cycle_stays_open_until_pending_drains() -> None:
    ledger = Ledger(_make_specs())
    # В цикле есть незавершённый элемент — компонента не закроется даже после seal.
    ledger.transition("s1", Event.ACCEPT)
    ledger.transition("s1", Event.START)
    ledger.seal_seed("s0")
    assert not ledger.is_closed("s1")
    ledger.transition("s1", Event.OK)
    assert ledger.is_closed("s1")


# --- hypothesis: случайные последовательности над графом с циклом ---------- #
STAGES = ("s0", "s1", "s2")


class LedgerMachine(RuleBasedStateMachine):
    """Случайные валидные переходы; инварианты проверяются после каждого шага."""

    def __init__(self) -> None:
        super().__init__()
        self.ledger = Ledger(_make_specs())
        self.sealed: set[str] = set()

    def _open(self, stage: str) -> bool:
        return not self.ledger.is_closed(stage)

    @rule(stage=st.sampled_from(STAGES))
    def accept(self, stage: str) -> None:
        if self._open(stage):
            self.ledger.transition(stage, Event.ACCEPT)

    @rule(stage=st.sampled_from(STAGES))
    def start(self, stage: str) -> None:
        if self.ledger.counts(stage).queued > 0:
            self.ledger.transition(stage, Event.START)

    @rule(stage=st.sampled_from(STAGES), which=st.sampled_from([Event.OK, Event.SKIP, Event.ERROR]))
    def finish(self, stage: str, which: Event) -> None:
        if self.ledger.counts(stage).in_flight > 0:
            self.ledger.transition(stage, which)

    @rule(stage=st.sampled_from(STAGES))
    def defer(self, stage: str) -> None:
        if self.ledger.counts(stage).in_flight > 0:
            self.ledger.transition(stage, Event.DEFER)

    @rule(stage=st.sampled_from(STAGES))
    def redeliver(self, stage: str) -> None:
        if self.ledger.counts(stage).deferred > 0:
            self.ledger.transition(stage, Event.REDELIVER)

    @rule(stage=st.sampled_from(STAGES))
    def cancel_queued(self, stage: str) -> None:
        if self.ledger.counts(stage).queued > 0:
            self.ledger.transition(stage, Event.CANCEL_QUEUED)

    @rule(stage=st.sampled_from(STAGES))
    def cancel_deferred(self, stage: str) -> None:
        if self.ledger.counts(stage).deferred > 0:
            self.ledger.transition(stage, Event.CANCEL_DEFERRED)

    @rule()
    def seal(self) -> None:
        if "s0" not in self.sealed:
            self.sealed.add("s0")
            self.ledger.seal_seed("s0")

    @invariant()
    def counter_invariant(self) -> None:
        assert self.ledger.invariant_holds()

    @invariant()
    def closing_consistent(self) -> None:
        for comp in self.ledger._components:
            both_zero = comp.pending == 0 and comp.writers == 0
            assert comp.closed == both_zero


TestLedgerMachine = LedgerMachine.TestCase  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]  # hypothesis нетипизирован


def test_stage_counts_derived() -> None:
    counts = StageCounts(ok=2, skip=1, error=1, cancelled=1, queued=3, in_flight=2, deferred=1)
    assert counts.done == 5
    assert counts.in_pipeline == 6
