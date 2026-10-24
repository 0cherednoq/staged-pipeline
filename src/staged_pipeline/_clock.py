"""Часы: протокол `Clock`, реальные `MonotonicClock` и тестовые `FakeClock`.

Время в библиотеке берётся только через `Clock` (architecture.md §4.3, §4.9):
это позволяет в тестах управлять им детерминированно через `FakeClock`, не
завися от реального `asyncio.sleep`.
"""

import asyncio
import heapq
import time
from typing import Protocol, runtime_checkable

__all__ = ["Clock", "FakeClock", "MonotonicClock"]


@runtime_checkable
class Clock(Protocol):
    """Источник времени для пайплайна.

    Реализации: `MonotonicClock` в проде, `FakeClock` в тестах.
    """

    def now(self) -> float:
        """Вернуть текущее время в секундах (монотонная шкала, не wall-clock)."""
        ...

    async def sleep(self, seconds: float) -> None:
        """Приостановить корутину на `seconds` секунд по шкале этих часов."""
        ...


class MonotonicClock:
    """Реальные часы поверх `time.monotonic` и `asyncio.sleep`."""

    def now(self) -> float:
        """Вернуть `time.monotonic()`."""
        return time.monotonic()

    async def sleep(self, seconds: float) -> None:
        """Делегировать `asyncio.sleep` (отрицательные значения — немедленно)."""
        await asyncio.sleep(max(0.0, seconds))


class FakeClock:
    """Виртуальные часы для тестов: время двигает только `advance()`.

    Время не идёт само: корутина, уснувшая через `sleep()`, проснётся лишь когда
    `advance()` доведёт виртуальное время до её дедлайна. Спящие будятся в порядке
    возрастания дедлайна (при равенстве — в порядке засыпания), что делает тесты
    детерминированными. Имя и смысл совпадают с `tallyho.testing.FakeClock`.
    """

    def __init__(self, start: float = 0.0) -> None:
        """Создать часы с начальным временем `start`.

        Args:
            start: стартовое значение `now()` в секундах.
        """
        self._now: float = start
        # Куча (дедлайн, порядковый номер, future): номер разрывает равенство
        # дедлайнов в пользу того, кто уснул раньше.
        self._sleepers: list[tuple[float, int, asyncio.Future[None]]] = []
        self._seq: int = 0

    def now(self) -> float:
        """Вернуть текущее виртуальное время."""
        return self._now

    async def sleep(self, seconds: float) -> None:
        """Уснуть до `now() + max(0, seconds)`; разбудит только `advance()`.

        Args:
            seconds: длительность сна; `<= 0` ставит дедлайн на текущий момент,
                поэтому `advance(0)` такого спящего разбудит.
        """
        deadline = self._now + max(0.0, seconds)
        future: asyncio.Future[None] = asyncio.get_running_loop().create_future()
        heapq.heappush(self._sleepers, (deadline, self._seq, future))
        self._seq += 1
        try:
            await future
        except asyncio.CancelledError:
            self._discard(future)
            raise

    def advance(self, seconds: float) -> None:
        """Сдвинуть время на `seconds` вперёд, разбудив всех дозревших спящих.

        Спящие с дедлайном `<= now + seconds` будятся в порядке дедлайна; на
        момент пробуждения `now()` равен дедлайну конкретного спящего. После —
        `now()` становится ровно `now + seconds`.

        Args:
            seconds: на сколько сдвинуть время (неотрицательное).

        Raises:
            ValueError: `seconds` отрицательно — время не идёт назад.
        """
        if seconds < 0:
            raise ValueError("advance() не принимает отрицательное время")
        target = self._now + seconds
        while self._sleepers and self._sleepers[0][0] <= target:
            deadline, _, future = heapq.heappop(self._sleepers)
            self._now = deadline
            if not future.done():
                future.set_result(None)
        self._now = target

    def _discard(self, future: asyncio.Future[None]) -> None:
        """Убрать отменённого спящего из кучи (вызывается при `CancelledError`)."""
        self._sleepers = [item for item in self._sleepers if item[2] is not future]
        heapq.heapify(self._sleepers)
