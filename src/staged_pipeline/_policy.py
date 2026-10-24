"""Политики обработки: ретраи, backoff, классификация исключений, предохранители.

architecture.md §4.4 (классификация исключения) и §4.5 (предохранители). Судьба
исключения объявляется на стадии (`retry=`, `skip_on=`, `error_on=`); сам разбор
делает `classify()` — чистая функция без побочных эффектов, которую зовёт воркер.
"""

import random
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from typing import Protocol, runtime_checkable

from staged_pipeline._errors import ConfigurationError, Error, Skip, validate_label

# Метка исхода задаётся строкой или функцией от исключения.
LabelMapper = str | Callable[[Exception], str]
ExcMap = Mapping[type[Exception], LabelMapper]


@runtime_checkable
class Backoff(Protocol):
    """Стратегия задержки перед ретраем: по номеру попытки — пауза в секундах."""

    def __call__(self, attempt: int) -> float:
        """Вернуть задержку перед попыткой `attempt` (1 — первый ретрай)."""
        ...


@dataclass(frozen=True, slots=True)
class Expo:
    """Экспоненциальный backoff: `base * 2**(attempt-1)`, но не больше `cap`.

    Attributes:
        base: базовая задержка первой попытки, секунды.
        cap: верхняя граница задержки, секунды.
        jitter: если истинно — случайная задержка в `[0, delay]` (anti-thundering-herd).
    """

    base: float
    cap: float
    jitter: bool = False

    def __call__(self, attempt: int) -> float:
        """Вычислить задержку для попытки `attempt` (1-based)."""
        raw = self.base * 2.0 ** (attempt - 1)
        delay = min(raw, self.cap)
        if self.jitter:
            delay = random.uniform(0.0, delay)  # noqa: S311  # не крипто: разброс ретраев
        return delay


@dataclass(frozen=True, slots=True)
class Fixed:
    """Постоянная задержка перед каждым ретраем."""

    seconds: float

    def __call__(self, attempt: int) -> float:  # noqa: ARG002  # сигнатура Backoff
        """Вернуть `seconds` независимо от номера попытки."""
        return self.seconds


def _normalize_on(
    on: type[Exception] | tuple[type[Exception], ...],
) -> tuple[type[Exception], ...]:
    """Привести `on` к кортежу классов исключений."""
    return on if isinstance(on, tuple) else (on,)


@dataclass(frozen=True, slots=True)
class Retry:
    """Политика ретрая для стадии (architecture.md §4.4).

    Attributes:
        on: класс или кортеж классов, при которых делается ретрай.
        attempts: максимальное число попыток (включая первую).
        backoff: стратегия задержки; по умолчанию `Expo(0.5, cap=10)`.
    """

    on: type[Exception] | tuple[type[Exception], ...]
    attempts: int
    backoff: Backoff = field(default_factory=lambda: Expo(0.5, cap=10.0))

    def matches(self, exc: Exception) -> bool:
        """Подходит ли исключение под ретрай по классу."""
        return isinstance(exc, _normalize_on(self.on))

    def classes(self) -> tuple[type[Exception], ...]:
        """Классы, для которых объявлен ретрай (для проверок конфигурации)."""
        return _normalize_on(self.on)


# --------------------------------------------------------------------------- #
# Решения классификатора (внутренние, наружу не экспортируются)               #
# --------------------------------------------------------------------------- #
@dataclass(frozen=True, slots=True)
class Deferral:
    """Исключение в `retry.on`, попытки есть: элемент уйдёт в отложенную очередь."""

    delay: float


@dataclass(frozen=True, slots=True)
class SkipOutcome:
    """Исход `skip(label)`."""

    label: str


@dataclass(frozen=True, slots=True)
class ErrorOutcome:
    """Исход `error(label)`."""

    label: str


@dataclass(frozen=True, slots=True)
class Failure:
    """Исключение не классифицировано — сбой пайплайна (fail-fast, D-001)."""


Decision = Deferral | SkipOutcome | ErrorOutcome | Failure


def _resolve_label(mapper: LabelMapper, exc: Exception) -> str:
    """Получить метку из элемента маппинга: строка либо функция от исключения."""
    label = mapper(exc) if callable(mapper) else mapper
    return validate_label(label)


def _mro_distance(exc: Exception, cls: type[Exception]) -> int:
    """Индекс `cls` в MRO исключения (чем меньше — тем ближе класс)."""
    mro = type(exc).__mro__
    return mro.index(cls) if cls in mro else len(mro)


def classify(
    exc: BaseException,
    *,
    retry: Retry | None,
    skip_on: ExcMap,
    error_on: ExcMap,
    attempt: int,
) -> Decision:
    """Классифицировать исключение хендлера (architecture.md §4.4).

    Порядок (ближайший класс по MRO):
        1. `Skip`/`Error` библиотеки → исход как есть.
        2. класс в `retry.on` и попытки не исчерпаны → `Deferral(backoff)`.
        3. `skip_on`/`error_on` → `SkipOutcome`/`ErrorOutcome` (сюда же — исчерпанный
           ретрай, если класс есть в маппинге).
        4. иначе — `Failure` (сбой пайплайна).

    `BaseException`, не являющееся `Exception` (в т.ч. `CancelledError`), не
    классифицируется никогда → `Failure`.

    Args:
        exc: пойманное исключение.
        retry: политика ретрая стадии или None.
        skip_on: маппинг класс→метка для исхода skip.
        error_on: маппинг класс→метка для исхода error.
        attempt: номер текущей попытки (1-based).

    Returns:
        Решение классификатора.
    """
    if not isinstance(exc, Exception):
        return Failure()
    if isinstance(exc, Skip):
        return SkipOutcome(exc.label)
    if isinstance(exc, Error):
        return ErrorOutcome(exc.label)
    if retry is not None and retry.matches(exc) and attempt < retry.attempts:
        return Deferral(retry.backoff(attempt))
    return _by_mapping(exc, skip_on, error_on)


def _by_mapping(exc: Exception, skip_on: ExcMap, error_on: ExcMap) -> Decision:
    """Выбрать исход по ближайшему в MRO классу из `skip_on`/`error_on`.

    Собирает всех подходящих кандидатов из обоих маппингов и берёт ближайшего по
    MRO. Один класс не может быть в обоих маппингах (см. `validate_policies`),
    поэтому минимум по дистанции единственен.
    """
    candidates: list[tuple[int, LabelMapper, bool]] = []
    for cls, mapper in skip_on.items():
        if isinstance(exc, cls):
            candidates.append((_mro_distance(exc, cls), mapper, True))
    for cls, mapper in error_on.items():
        if isinstance(exc, cls):
            candidates.append((_mro_distance(exc, cls), mapper, False))
    if not candidates:
        return Failure()
    _, mapper, is_skip = min(candidates, key=lambda item: item[0])
    label = _resolve_label(mapper, exc)
    return SkipOutcome(label) if is_skip else ErrorOutcome(label)


def validate_policies(retry: Retry | None, skip_on: ExcMap, error_on: ExcMap) -> None:
    """Проверить непротиворечивость политик при создании стадии (§4.4).

    Raises:
        ConfigurationError: класс есть и в `skip_on`, и в `error_on`, либо класс
            одновременно в `retry.on` и в одном из `*_on`.
    """
    both = set(skip_on) & set(error_on)
    if both:
        names = ", ".join(sorted(cls.__name__ for cls in both))
        raise ConfigurationError(f"класс одновременно в skip_on и error_on: {names}")
    if retry is not None:
        retry_classes = set(retry.classes())
        clash = retry_classes & (set(skip_on) | set(error_on))
        if clash:
            names = ", ".join(sorted(cls.__name__ for cls in clash))
            raise ConfigurationError(f"класс одновременно в retry.on и *_on: {names}")


# --------------------------------------------------------------------------- #
# Предохранители (architecture.md §4.5)                                       #
# --------------------------------------------------------------------------- #
@dataclass(frozen=True, slots=True)
class BreakerStats:
    """Снимок счётчиков стадии для проверки предохранителя.

    Attributes:
        ok: число успешных исходов.
        error: число исходов error.
        labels: счётчик всех меток исходов (ok/skip/error).
        closing: стадия закрывается (проверка при завершении).
    """

    ok: int
    error: int
    labels: Mapping[str, int]
    closing: bool


@runtime_checkable
class Breaker(Protocol):
    """Предохранитель: по снимку счётчиков решает, пора ли ронять стадию."""

    def tripped(self, stats: BreakerStats) -> bool:
        """Сработал ли предохранитель на данном снимке."""
        ...

    def reason(self, stats: BreakerStats) -> str:
        """Человекочитаемая причина срабатывания (для заметки исключения)."""
        ...


@dataclass(frozen=True, slots=True)
class ErrorRate:
    """Срабатывает во время работы, когда доля ошибок достигает порога.

    Attributes:
        threshold: порог доли ошибок в `[0, 1]`.
        min_samples: минимум исходов (ok+error) до первой проверки.
        labels: если задано — считать ошибками только эти метки error.
    """

    threshold: float
    min_samples: int
    labels: frozenset[str] | None = None

    def _counts(self, stats: BreakerStats) -> tuple[int, int]:
        """Вернуть (ошибки, выборка) с учётом фильтра `labels`."""
        if self.labels is None:
            errors = stats.error
        else:
            errors = sum(stats.labels.get(label, 0) for label in self.labels)
        return errors, stats.ok + errors

    def tripped(self, stats: BreakerStats) -> bool:
        """Доля ошибок `>= threshold` при выборке `>= min_samples`."""
        errors, samples = self._counts(stats)
        if samples < self.min_samples or samples == 0:
            return False
        return errors / samples >= self.threshold

    def reason(self, stats: BreakerStats) -> str:
        """Строка вида `ErrorRate(threshold=0.9, min_samples=20): 20/20 errors`."""
        errors, samples = self._counts(stats)
        return (
            f"ErrorRate(threshold={self.threshold}, min_samples={self.min_samples}): "
            f"{errors}/{samples} errors"
        )


@dataclass(frozen=True, slots=True)
class NoSuccess:
    """Срабатывает при закрытии, если не было ни одного успеха, а ошибки были.

    Attributes:
        count_labels: метки skip, которые тоже считаются «что-то сделали» (напр.
            `resumed`), чтобы прогон из одних resumed не ронял стадию (§8).
    """

    count_labels: frozenset[str] = frozenset()

    def _successes(self, stats: BreakerStats) -> int:
        """Успехи: ok плюс исходы с метками из `count_labels`."""
        return stats.ok + sum(stats.labels.get(label, 0) for label in self.count_labels)

    def tripped(self, stats: BreakerStats) -> bool:
        """При закрытии: успехов нет, а ошибки есть."""
        return stats.closing and self._successes(stats) == 0 and stats.error > 0

    def reason(self, stats: BreakerStats) -> str:
        """Строка вида `NoSuccess: 0 ok, 5 errors`."""
        return f"NoSuccess: {self._successes(stats)} ok, {stats.error} errors"


__all__ = [
    "Backoff",
    "Breaker",
    "BreakerStats",
    "ErrorRate",
    "Expo",
    "Fixed",
    "NoSuccess",
    "Retry",
]
