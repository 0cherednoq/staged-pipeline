"""Тесты иерархии исключений и валидации меток исходов (T1.1)."""

import pytest

from staged_pipeline._errors import (
    ConfigurationError,
    Error,
    Outcome,
    RoutingError,
    Skip,
    StageAborted,
    StageClosedError,
    StagedPipelineError,
    TopologyError,
)

LIBRARY_ERRORS = [
    ConfigurationError,
    TopologyError,
    RoutingError,
    StageAborted,
    StageClosedError,
]


@pytest.mark.parametrize("exc", LIBRARY_ERRORS)
def test_library_errors_subclass_base(exc: type[StagedPipelineError]) -> None:
    assert issubclass(exc, StagedPipelineError)


def test_outcomes_are_separate_from_library_errors() -> None:
    # Skip/Error — исходы, а не сбои библиотеки (§4.3).
    assert issubclass(Skip, Outcome)
    assert issubclass(Error, Outcome)
    assert not issubclass(Outcome, StagedPipelineError)
    assert not issubclass(Skip, StagedPipelineError)


@pytest.mark.parametrize("outcome_cls", [Skip, Error])
def test_label_stored(outcome_cls: type[Outcome]) -> None:
    outcome = outcome_cls("deleted")
    assert outcome.label == "deleted"
    assert str(outcome) == "deleted"


@pytest.mark.parametrize("outcome_cls", [Skip, Error])
@pytest.mark.parametrize("bad", ["", "   ", "x" * 201])
def test_invalid_label_raises_configuration_error(outcome_cls: type[Outcome], bad: str) -> None:
    with pytest.raises(ConfigurationError):
        outcome_cls(bad)


def test_label_at_limit_is_accepted() -> None:
    label = "x" * 200
    assert Skip(label).label == label
