"""Очередь стадии: готовые элементы (FIFO) и отложенные ретраи (heap по времени).

architecture.md §7.1. Своя очередь вместо `asyncio.Queue`, потому что нужны:
отложенные по `not_before` элементы, учёт остатка при отмене (его `Queue.shutdown`
теряет молча) и проверка закрытия без гонок. Время берётся через `Clock`, поэтому
в тестах поведение детерминировано на `FakeClock`.
"""

import asyncio
import contextlib
import heapq
from collections import deque
from dataclasses import dataclass

from staged_pipeline._clock import Clock
from staged_pipeline._errors import StageClosedError

__all__ = ["Envelope", "Inbox"]


@dataclass(slots=True)
class Envelope[T]:
    """Элемент в пути по стадии вместе с метаданными обработки.

    Attributes:
        item: полезная нагрузка (то, что увидит хендлер).
        attempt: номер текущей попытки (1 — первая), растёт при ретрае.
    """

    item: T
    attempt: int = 1


class Inbox[T]:
    """Вход стадии: готовые элементы и отложенные ретраи.

    `get()` отдаёт готовые в порядке FIFO; отложенный элемент становится готовым,
    когда `Clock.now()` достигает его `not_before`. Ожидающий `get()` просыпается
    либо от нового `put`/`defer`, либо по времени ближайшего отложенного, либо при
    `close()`/`cancel()`.
    """

    def __init__(self, clock: Clock, *, maxsize: int | None = None) -> None:
        """Создать вход.

        Args:
            clock: источник времени для отложенных элементов.
            maxsize: ограничение числа готовых элементов (bounded); None — без
                ограничения (умолчание, architecture.md D-003).
        """
        self._clock: Clock = clock
        self._maxsize: int | None = maxsize
        self._ready: deque[Envelope[T]] = deque()
        # Куча (not_before, порядковый номер, конверт): номер стабилизирует порядок.
        self._deferred: list[tuple[float, int, Envelope[T]]] = []
        self._seq: int = 0
        self._get_waiters: deque[asyncio.Future[None]] = deque()
        self._put_waiters: deque[asyncio.Future[None]] = deque()
        self._closed: bool = False

    @property
    def len_ready(self) -> int:
        """Число готовых элементов."""
        return len(self._ready)

    @property
    def len_deferred(self) -> int:
        """Число отложенных элементов."""
        return len(self._deferred)

    @property
    def closed(self) -> bool:
        """Закрыт ли вход (новые элементы не принимаются)."""
        return self._closed

    def put_nowait(self, env: Envelope[T]) -> None:
        """Положить готовый элемент немедленно, без учёта лимита (seed/unbounded).

        Raises:
            StageClosedError: вход закрыт.
        """
        if self._closed:
            raise StageClosedError("попытка put в закрытый вход")
        self._ready.append(env)
        self._wake_one(self._get_waiters)

    async def put(self, env: Envelope[T]) -> None:
        """Положить готовый элемент, ожидая место, если вход bounded и полон.

        Raises:
            StageClosedError: вход закрыт (в т.ч. если закрылся во время ожидания).
        """
        while self._maxsize is not None and len(self._ready) >= self._maxsize:
            if self._closed:
                raise StageClosedError("попытка put в закрытый вход")
            waiter = asyncio.get_running_loop().create_future()
            self._put_waiters.append(waiter)
            try:
                await waiter
            finally:
                with contextlib.suppress(ValueError):
                    self._put_waiters.remove(waiter)
        if self._closed:
            raise StageClosedError("попытка put в закрытый вход")
        self._ready.append(env)
        self._wake_one(self._get_waiters)

    def defer(self, env: Envelope[T], not_before: float) -> None:
        """Отложить элемент до момента `not_before` по шкале `Clock`.

        Raises:
            StageClosedError: вход закрыт.
        """
        if self._closed:
            raise StageClosedError("попытка defer в закрытый вход")
        heapq.heappush(self._deferred, (not_before, self._seq, env))
        self._seq += 1
        # Разбудить ожидающих: у отложенного может быть более ранний дедлайн.
        self._wake_one(self._get_waiters)

    async def get(self) -> Envelope[T] | None:
        """Взять готовый элемент; None — вход закрыт и пуст.

        Ждёт при пустом входе: нового `put`/`defer`, дозревания ближайшего
        отложенного по `Clock` или закрытия.
        """
        while True:
            self._promote_due()
            if self._ready:
                env = self._ready.popleft()
                self._wake_one(self._put_waiters)
                return env
            if self._closed and not self._deferred:
                return None
            await self._wait_for_work()

    async def _wait_for_work(self) -> None:
        """Подождать нового элемента, дозревания отложенного или закрытия."""
        loop = asyncio.get_running_loop()
        waiter: asyncio.Future[None] = loop.create_future()
        self._get_waiters.append(waiter)
        timer: asyncio.Task[None] | None = None
        if self._deferred:
            delay = max(0.0, self._deferred[0][0] - self._clock.now())
            timer = asyncio.ensure_future(self._clock.sleep(delay))
        try:
            waitables: set[asyncio.Future[None]] = {waiter}
            if timer is not None:
                waitables.add(timer)
            await asyncio.wait(waitables, return_when=asyncio.FIRST_COMPLETED)
        finally:
            with contextlib.suppress(ValueError):
                self._get_waiters.remove(waiter)
            if timer is not None and not timer.done():
                timer.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await timer

    def _promote_due(self) -> None:
        """Перенести все дозревшие отложенные элементы в очередь готовых."""
        now = self._clock.now()
        while self._deferred and self._deferred[0][0] <= now:
            _, _, env = heapq.heappop(self._deferred)
            self._ready.append(env)

    def close(self) -> None:
        """Закрыть вход: новые элементы не принимаются, ждущие разбужены.

        Идемпотентно. Уже лежащие готовые и отложенные элементы остаются —
        `get()` их доотдаст, затем вернёт None.
        """
        self._closed = True
        self._wake_all()

    def cancel(self) -> list[Envelope[T]]:
        """Закрыть вход и вернуть остаток (для учёта как `cancelled`).

        Returns:
            Все готовые и отложенные элементы в порядке «сначала готовые (FIFO),
            затем отложенные по возрастанию `not_before`». Очередь опустошается.
        """
        self._closed = True
        remainder = list(self._ready)
        remainder.extend(env for _, _, env in sorted(self._deferred))
        self._ready.clear()
        self._deferred.clear()
        self._wake_all()
        return remainder

    @staticmethod
    def _wake_one(waiters: deque[asyncio.Future[None]]) -> None:
        """Разбудить одного ждущего (первого не завершённого)."""
        while waiters:
            waiter = waiters.popleft()
            if not waiter.done():
                waiter.set_result(None)
                return

    def _wake_all(self) -> None:
        """Разбудить всех ждущих `get` и `put`."""
        for waiters in (self._get_waiters, self._put_waiters):
            for waiter in list(waiters):
                if not waiter.done():
                    waiter.set_result(None)
            waiters.clear()
