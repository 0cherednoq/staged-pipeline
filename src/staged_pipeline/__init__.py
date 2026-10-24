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
from staged_pipeline._limit import Limit, Limiter
from staged_pipeline._pipeline import Pipeline
from staged_pipeline._policy import (
    Backoff,
    Breaker,
    ErrorRate,
    Expo,
    Fixed,
    NoSuccess,
    Retry,
)
from staged_pipeline._stage import Feeder, SeenStore, SetSeenStore, Stage, StageContext

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
    "Feeder",
    "Fixed",
    "Limit",
    "Limiter",
    "MonotonicClock",
    "NoSuccess",
    "Outcome",
    "Pipeline",
    "Retry",
    "RoutingError",
    "SeenStore",
    "SetSeenStore",
    "Skip",
    "Stage",
    "StageAborted",
    "StageClosedError",
    "StageContext",
    "StagedPipelineError",
    "TopologyError",
    "__version__",
]
