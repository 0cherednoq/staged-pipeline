"""Супервизор: одна TaskGroup на весь пайплайн, первый сбой наружу плоско (§7.3).

Все воркеры всех стадий живут в одной `asyncio.TaskGroup`. При сбое записывается
**первое хронологически** исключение (а не произвольное из `ExceptionGroup`),
остальные воркеры отменяются, наружу уходит это исходное исключение без обёртки
в группу. Намеренная отмена (`cancel_all`, напр. из `Pipeline.cancel`) гасит
воркеров и завершается тихо; внешний `CancelledError` (отмена задачи `run()`) —
пробрасывается как есть.
"""

import asyncio
from collections.abc import Callable, Coroutine, Sequence

__all__ = ["Supervisor"]


def _leaves(exc: BaseException) -> list[BaseException]:
    """Развернуть вложенные группы исключений в плоский список листьев."""
    if isinstance(exc, BaseExceptionGroup):
        result: list[BaseException] = []
        for sub in exc.exceptions:
            result.extend(_leaves(sub))
        return result
    return [exc]


class Supervisor:
    """Запускает воркеры в одной TaskGroup и разворачивает первый сбой плоско."""

    def __init__(self) -> None:
        """Создать супервизор без зафиксированного сбоя."""
        self._failure: BaseException | None = None
        self._tasks: list[asyncio.Task[None]] = []
        self._stopping: bool = False

    @property
    def failure(self) -> BaseException | None:
        """Первое зафиксированное исключение (или None)."""
        return self._failure

    @property
    def tasks(self) -> list[asyncio.Task[None]]:
        """Запущенные задачи-воркеры."""
        return self._tasks

    def record(self, exc: BaseException) -> None:
        """Зафиксировать сбой; остаётся первое хронологически вызванное."""
        if self._failure is None:
            self._failure = exc

    def cancel_all(self) -> None:
        """Намеренно отменить всех воркеров (из `Pipeline.cancel`/drain по таймауту)."""
        self._stopping = True
        for task in self._tasks:
            task.cancel()

    async def run(self, factories: Sequence[Callable[[], Coroutine[object, object, None]]]) -> None:
        """Запустить все воркеры; при сбое бросить первое исходное исключение.

        Args:
            factories: фабрики корутин-воркеров (по одной на воркер стадии).

        Raises:
            BaseException: первое зафиксированное исключение воркера; внешний
                `CancelledError` пробрасывается как есть; намеренная отмена —
                тихое завершение.
        """
        try:
            async with asyncio.TaskGroup() as group:
                self._tasks = [group.create_task(factory()) for factory in factories]
        except BaseException as eg:
            leaves = _leaves(eg)
            only_cancelled = all(isinstance(leaf, asyncio.CancelledError) for leaf in leaves)
            if self._stopping and only_cancelled:
                return  # намеренная остановка (cancel/drain-таймаут)
            if self._failure is not None:
                raise self._failure from None
            raise
