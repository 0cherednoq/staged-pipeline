"""Тесты политик: backoff, классификация исключений, предохранители (T1.2)."""

import random

import pytest

from staged_pipeline._errors import ConfigurationError, Error, Skip
from staged_pipeline._policy import (
    BreakerStats,
    Deferral,
    ErrorOutcome,
    ErrorRate,
    Expo,
    Failure,
    Fixed,
    NoSuccess,
    Retry,
    SkipOutcome,
    classify,
    validate_policies,
)


class NetError(Exception):
    pass


class TimeoutErr(NetError):
    pass


class NotFound(Exception):
    pass


class ParseError(Exception):
    pass


# --------------------------------------------------------------------------- #
# backoff                                                                     #
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize(
    ("attempt", "expected"),
    [(1, 0.5), (2, 1.0), (3, 2.0), (4, 4.0)],
)
def test_expo_growth(attempt: int, expected: float) -> None:
    assert Expo(0.5, cap=100.0)(attempt) == expected


def test_expo_capped() -> None:
    assert Expo(1.0, cap=3.0)(10) == 3.0


def test_expo_jitter_within_bounds() -> None:
    random.seed(1234)
    backoff = Expo(1.0, cap=8.0, jitter=True)
    for attempt in range(1, 5):
        delay = backoff(attempt)
        assert 0.0 <= delay <= min(2.0 ** (attempt - 1), 8.0)


def test_fixed_backoff() -> None:
    backoff = Fixed(0.25)
    assert backoff(1) == 0.25
    assert backoff(9) == 0.25


def test_retry_default_backoff_is_expo() -> None:
    retry = Retry(on=NetError, attempts=2)
    assert isinstance(retry.backoff, Expo)


# --------------------------------------------------------------------------- #
# классификация                                                               #
# --------------------------------------------------------------------------- #
def _classify(exc: BaseException, *, retry: Retry | None = None, attempt: int = 1) -> object:
    return classify(
        exc,
        retry=retry,
        skip_on={NotFound: "deleted"},
        error_on={ParseError: "bad_mime"},
        attempt=attempt,
    )


def test_skip_outcome_passes_through() -> None:
    assert _classify(Skip("done")) == SkipOutcome("done")


def test_error_outcome_passes_through() -> None:
    assert _classify(Error("boom")) == ErrorOutcome("boom")


def test_skip_on_mapping() -> None:
    assert _classify(NotFound()) == SkipOutcome("deleted")


def test_error_on_mapping() -> None:
    assert _classify(ParseError()) == ErrorOutcome("bad_mime")


def test_unmapped_exception_fails() -> None:
    assert _classify(RuntimeError("x")) == Failure()


def test_base_exception_not_classified() -> None:
    # BaseException (не Exception) никогда не классифицируется (§4.3).
    assert _classify(KeyboardInterrupt()) == Failure()


def test_retry_defers_while_attempts_remain() -> None:
    retry = Retry(on=NetError, attempts=3, backoff=Fixed(0.5))
    assert _classify(NetError(), retry=retry, attempt=1) == Deferral(0.5)
    assert _classify(NetError(), retry=retry, attempt=2) == Deferral(0.5)


def test_retry_exhausted_falls_through_to_mapping() -> None:
    # NetError нет в *_on → после исчерпания ретраев это Failure (fail-fast).
    retry = Retry(on=NetError, attempts=3, backoff=Fixed(0.5))
    assert _classify(NetError(), retry=retry, attempt=3) == Failure()


def test_retry_exhausted_mapped_to_error() -> None:
    retry = Retry(on=TimeoutErr, attempts=2, backoff=Fixed(0.1))
    decision = classify(
        TimeoutErr(),
        retry=retry,
        skip_on={},
        error_on={NetError: "net_failed"},
        attempt=2,
    )
    assert decision == ErrorOutcome("net_failed")


def test_mro_nearest_class_wins() -> None:
    # ValueError ближе по MRO, чем Exception → выигрывает error_on.
    decision = classify(
        ValueError("x"),
        retry=None,
        skip_on={Exception: "generic"},
        error_on={ValueError: "specific"},
        attempt=1,
    )
    assert decision == ErrorOutcome("specific")


def test_callable_label_mapper() -> None:
    decision = classify(
        ParseError(),
        retry=None,
        skip_on={},
        error_on={ParseError: lambda e: f"parse:{type(e).__name__}"},
        attempt=1,
    )
    assert decision == ErrorOutcome("parse:ParseError")


# --------------------------------------------------------------------------- #
# проверка конфигурации политик                                               #
# --------------------------------------------------------------------------- #
def test_class_in_both_maps_rejected() -> None:
    with pytest.raises(ConfigurationError, match="skip_on и error_on"):
        validate_policies(None, {NotFound: "a"}, {NotFound: "b"})


def test_class_in_retry_and_mapping_rejected() -> None:
    with pytest.raises(ConfigurationError, match="retry"):
        validate_policies(Retry(on=NetError, attempts=2), {}, {NetError: "x"})


def test_valid_policies_pass() -> None:
    validate_policies(Retry(on=NetError, attempts=2), {NotFound: "a"}, {ParseError: "b"})


# --------------------------------------------------------------------------- #
# предохранители                                                              #
# --------------------------------------------------------------------------- #
def _stats(
    *, ok: int, error: int, labels: dict[str, int] | None = None, closing: bool
) -> BreakerStats:
    return BreakerStats(ok=ok, error=error, labels=labels or {}, closing=closing)


def test_error_rate_trips_on_all_errors() -> None:
    breaker = ErrorRate(1.0, min_samples=20)
    assert breaker.tripped(_stats(ok=0, error=20, closing=False))


def test_error_rate_below_min_samples_does_not_trip() -> None:
    breaker = ErrorRate(1.0, min_samples=20)
    assert not breaker.tripped(_stats(ok=0, error=19, closing=False))


def test_error_rate_below_threshold_does_not_trip() -> None:
    breaker = ErrorRate(0.9, min_samples=10)
    assert not breaker.tripped(_stats(ok=5, error=5, closing=False))


def test_error_rate_labels_filter() -> None:
    breaker = ErrorRate(1.0, min_samples=3, labels=frozenset({"store_failed"}))
    stats = _stats(ok=0, error=3, labels={"store_failed": 3}, closing=False)
    assert breaker.tripped(stats)
    other = _stats(ok=0, error=3, labels={"other": 3}, closing=False)
    assert not other.labels.get("store_failed")
    assert not breaker.tripped(other)


def test_error_rate_reason_text() -> None:
    breaker = ErrorRate(0.9, min_samples=20)
    assert "20/20 errors" in breaker.reason(_stats(ok=0, error=20, closing=False))


def test_no_success_trips_at_close() -> None:
    breaker = NoSuccess()
    assert breaker.tripped(_stats(ok=0, error=5, closing=True))


def test_no_success_not_before_close() -> None:
    breaker = NoSuccess()
    assert not breaker.tripped(_stats(ok=0, error=5, closing=False))


def test_no_success_quiet_when_some_ok() -> None:
    breaker = NoSuccess()
    assert not breaker.tripped(_stats(ok=1, error=5, closing=True))


def test_no_success_count_labels_counts_as_success() -> None:
    breaker = NoSuccess(count_labels=frozenset({"resumed"}))
    stats = _stats(ok=0, error=5, labels={"resumed": 3}, closing=True)
    assert not breaker.tripped(stats)


def test_no_success_reason_text() -> None:
    breaker = NoSuccess()
    assert breaker.reason(_stats(ok=0, error=5, closing=True)) == "NoSuccess: 0 ok, 5 errors"
