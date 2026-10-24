"""Тесты прогресса: expected по §6, ETA, watch, Observer, warn_queued (T4.1)."""

import asyncio

from staged_pipeline._accounting import Event, Ledger
from staged_pipeline._clock import FakeClock
from staged_pipeline._graph import StageSpec
from staged_pipeline._pipeline import Pipeline
from staged_pipeline._progress import EwmaTracker, PipelineView, StageState, build_pipeline_view

TIMEOUT = 3.0


# --- доменные хендлеры для StageSpec --------------------------------------- #
async def h_sink(n: int) -> None:  # noqa: ARG001
    return None


async def h_fanout(n: int) -> int:
    return n


def _view(
    specs: list[StageSpec], ledger: Ledger, expects: dict[str, int] | None = None
) -> PipelineView:
    clock = FakeClock()
    trackers = {s.name: EwmaTracker(clock) for s in specs}
    return build_pipeline_view(
        "p", specs, ledger, trackers, expects or {}, stopped=None, failed=False
    )


def _run_source(ledger: Ledger, name: str, count: int) -> None:
    for _ in range(count):
        ledger.transition(name, Event.ACCEPT)
        ledger.transition(name, Event.START)
        ledger.transition(name, Event.OK)


# --- expected по строкам §6 ------------------------------------------------ #
def test_expected_row1_closed_is_exact() -> None:
    a = StageSpec(name="a", handler=h_sink, seeded=True)
    ledger = Ledger([a], seed_inputs={"a": 1})
    _run_source(ledger, "a", 2)
    ledger.seal_seed("a")  # writers 0, pending 0 → closed
    view = _view([a], ledger)
    assert view["a"].expected == 2
    assert not view["a"].expected_is_estimate
    assert view["a"].estimate_basis == "closed"


def test_expected_row2_hint() -> None:
    a = StageSpec(name="a", handler=h_sink, seeded=True)
    ledger = Ledger([a], seed_inputs={"a": 1})  # seed не запечатан → открыта
    _run_source(ledger, "a", 3)
    view = _view([a], ledger, expects={"a": 100})
    assert view["a"].expected == 100
    assert view["a"].expected_is_estimate
    assert view["a"].estimate_basis == "hint"


def test_expected_row3_estimate_from_writers() -> None:
    a = StageSpec(name="a", handler=h_fanout, seeded=True)
    b = StageSpec(name="b", handler=h_sink)
    a.to(b)
    ledger = Ledger([a, b], seed_inputs={"a": 1})  # a открыта (seed не запечатан)
    for _ in range(10):
        ledger.transition("a", Event.ACCEPT)
        ledger.transition("a", Event.START)
        for _child in range(2):
            ledger.emit("a", "b")
            ledger.transition("b", Event.ACCEPT)
        ledger.transition("a", Event.OK)
    view = _view([a, b], ledger, expects={"a": 10})
    # a.expected = 10 (hint), a.done=10, emitted[b]=20 → b ≈ 20/10 * 10 = 20.
    assert view["b"].expected == 20
    assert view["b"].expected_is_estimate
    assert view["b"].estimate_basis == "estimate"


def test_expected_row4_cycle_is_none() -> None:
    a = StageSpec(name="a", handler=h_fanout, seeded=True)
    a.to(a)  # самопетля → цикл
    ledger = Ledger([a], seed_inputs={"a": 1})
    ledger.transition("a", Event.ACCEPT)
    ledger.transition("a", Event.START)
    view = _view([a], ledger)
    assert view["a"].expected is None
    assert view["a"].estimate_basis == "cycle"


def test_expected_row5_unknown_is_none() -> None:
    a = StageSpec(name="a", handler=h_sink, seeded=True)
    ledger = Ledger([a], seed_inputs={"a": 1})  # открыта, без подсказки, без писателей
    _run_source(ledger, "a", 1)
    view = _view([a], ledger)
    assert view["a"].expected is None
    assert view["a"].estimate_basis == "unknown"


# --- ETA ------------------------------------------------------------------- #
def test_eta_from_rate() -> None:
    clock = FakeClock()
    tracker = EwmaTracker(clock)
    tracker.record(0.1)
    clock.advance(1.0)
    tracker.record(0.1)  # dt=1 → rate 1/с
    assert tracker.rate == 1.0

    a = StageSpec(name="a", handler=h_sink, seeded=True)
    ledger = Ledger([a], seed_inputs={"a": 1})
    for _ in range(2):
        ledger.transition("a", Event.ACCEPT)
        ledger.transition("a", Event.START)
        ledger.transition("a", Event.OK)
    view = build_pipeline_view(
        "p", [a], ledger, {"a": tracker}, {"a": 10}, stopped=None, failed=False
    )
    # expected 10, done 2, rate 1 → eta = 8.
    assert view["a"].eta == 8.0


# --- watch / view через реальный прогон ------------------------------------ #
async def test_watch_ends_with_terminal_snapshot() -> None:
    async def handler(n: int) -> None:  # noqa: ARG001
        return None

    pipe = Pipeline("watch")
    pipe.stage("s", handler).seed([1, 2, 3])
    await asyncio.wait_for(pipe.run(), TIMEOUT)

    views = [view async for view in pipe.watch(interval=0.01)]
    assert views
    assert all(s.state is StageState.CLOSED for s in views[-1].stages.values())


async def test_report_fields() -> None:
    async def handler(n: int) -> None:  # noqa: ARG001
        return None

    pipe = Pipeline("report")
    pipe.stage("s", handler).seed([1, 2])
    report = await asyncio.wait_for(pipe.run(), TIMEOUT)
    assert report.stopped is None
    assert report.final
    assert report.stages["s"].ok == 2
    assert report.duration >= 0


# --- Observer / warn_queued ------------------------------------------------ #
class _Recorder:
    def __init__(self) -> None:
        self.closed: list[str] = []
        self.finished: list[tuple[str, str]] = []
        self.warnings: list[tuple[str, int]] = []
        self.tripped: list[str] = []

    def stage_closed(self, stage: str) -> None:
        self.closed.append(stage)

    def item_finished(self, stage: str, key: object, outcome: str, exc: object) -> None:
        del key, exc
        self.finished.append((stage, outcome))

    def breaker_tripped(self, stage: str, reason: str) -> None:
        del reason
        self.tripped.append(stage)

    def queued_warning(self, stage: str, queued: int) -> None:
        self.warnings.append((stage, queued))


async def test_observer_receives_events() -> None:
    async def handler(n: int) -> None:  # noqa: ARG001
        return None

    rec = _Recorder()
    pipe = Pipeline("obs", observer=rec)
    pipe.stage("s", handler).seed([1, 2])
    await asyncio.wait_for(pipe.run(), TIMEOUT)

    assert "s" in rec.closed
    assert ("s", "ok") in rec.finished


async def test_warn_queued_fires_once() -> None:
    async def handler(n: int) -> None:  # noqa: ARG001
        return None

    rec = _Recorder()
    pipe = Pipeline("warn", observer=rec)
    pipe.stage("s", handler, workers=1, warn_queued=2).seed(list(range(10)))
    await asyncio.wait_for(pipe.run(), TIMEOUT)

    assert rec.warnings  # очередь превышала порог
    assert rec.warnings[0][0] == "s"
