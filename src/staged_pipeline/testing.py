"""Тест-утилиты: прогон одной стадии и `FakeClock` (architecture.md §4.9).

`run_stage` собирает одностадийный пайплайн, засевает элементы, собирает выход
и возвращает его вместе со снимком стадии. `FakeClock` реэкспортируется, чтобы
тесты пользователя управляли временем, как во внутренних тестах.
"""

from collections.abc import Callable, Iterable
from dataclasses import dataclass

from staged_pipeline._clock import Clock, FakeClock
from staged_pipeline._pipeline import Pipeline
from staged_pipeline._progress import StageView

__all__ = ["FakeClock", "StageRun", "run_stage"]


@dataclass(frozen=True, slots=True)
class StageRun:
    """Результат `run_stage`: собранный выход и снимок стадии."""

    outputs: list[object]
    view: StageView


async def run_stage(
    handler: Callable[..., object],
    items: Iterable[object],
    *,
    clock: Clock | None = None,
    **stage_kwargs: object,
) -> StageRun:
    """Прогнать одну стадию над `items` и вернуть выход и снимок (§4.9).

    Args:
        handler: хендлер стадии (как в `Pipeline.stage`).
        items: засеваемые элементы.
        clock: часы (по умолчанию реальные); передайте `FakeClock()` для ретраев.
        stage_kwargs: прочие параметры стадии (`retry`, `key`, `skip_on`, ...).

    Returns:
        `StageRun` с собранным выходом стадии и её `StageView`.
    """
    outputs: list[object] = []

    async def _collect(value: object) -> None:
        outputs.append(value)

    pipe = Pipeline("run_stage", clock=clock)
    stage = pipe.stage("stage", handler, **stage_kwargs)  # type: ignore[arg-type]  # pyright: ignore[reportArgumentType]  # прокидываем kwargs
    sink = pipe.stage("_collect", _collect)
    stage.to(sink)
    stage.seed(items)
    report = await pipe.run()
    return StageRun(outputs=outputs, view=report.stages["stage"])
