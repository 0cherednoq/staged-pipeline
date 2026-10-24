"""staged-pipeline — декларативный конвейер стадий поверх asyncio.

Публичный API расширяется по мере реализации (см. docs/plan/PLAN.md). Сейчас
доступны версия, иерархия исключений, исходы `Skip`/`Error` и часы.
"""

from importlib.metadata import PackageNotFoundError, version

from staged_pipeline._clock import Clock, MonotonicClock
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
from staged_pipeline._policy import (
    Backoff,
    Breaker,
    ErrorRate,
    Expo,
    Fixed,
    NoSuccess,
    Retry,
)

try:
    __version__ = version("staged-pipeline")
except PackageNotFoundError:  # pragma: no cover - пакет не установлен (запуск из исходников)
    __version__ = "0.0.0.dev0"

__all__ = [
    "Backoff",
    "Breaker",
    "Clock",
    "ConfigurationError",
    "Error",
    "ErrorRate",
    "Expo",
    "Fixed",
    "MonotonicClock",
    "NoSuccess",
    "Outcome",
    "Retry",
    "RoutingError",
    "Skip",
    "StageAborted",
    "StageClosedError",
    "StagedPipelineError",
    "TopologyError",
    "__version__",
]
