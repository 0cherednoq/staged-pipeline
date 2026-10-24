"""Супервизор: одна TaskGroup на весь пайплайн, первый сбой наружу плоско (§7.3).

Все воркеры всех стадий живут в одной `asyncio.TaskGroup`. При сбое записывается
**первое хронологически** исключение (а не произвольное из `ExceptionGroup`),
остальные воркеры отменяются, наружу уходит это исходное исключение без обёртки
в группу. Внешний `CancelledError` (отмена задачи `run()`) не подменяется.
"""

import asyncio
from collections.abc import Callable, Coroutine, Sequence

__all__ = ["Supervisor"]


class Supervisor:
    """Запускает воркеры в одной TaskGroup и разворачивает первый сбой плоско."""

    def __init__(self) -> None:
        """Создать супервизор без зафиксированного сбоя."""
        self._failure: BaseException | None = None

    @property
    def failure(self) -> BaseException | None:
        """Первое зафиксированное исключение (или None)."""
        return self._failure

    def record(self, exc: BaseException) -> None:
        """Зафиксировать сбой; остаётся первое хронологически вызванное."""
        if self._failure is None:
            self._failure = exc

    async def run(self, factories: Sequence[Callable[[], Coroutine[object, object, None]]]) -> None:
        """Запустить все воркеры; при сбое бросить первое исходное исключение.

        Args:
            factories: фабрики корутин-воркеров (по одной на воркер стадии).

        Raises:
            BaseException: первое зафиксированное исключение воркера; внешний
                `CancelledError` пробрасывается как есть.
        """
        try:
            async with asyncio.TaskGroup() as group:
                for factory in factories:
                    group.create_task(factory())
        except* Exception:
            # Группу поднял TaskGroup; первое хронологически — в self._failure.
            if self._failure is not None:
                raise self._failure from None
            raise
