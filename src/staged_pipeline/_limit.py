"""Лимиты: протокол `Limiter` и `Limit` — семафор конкурентности + token bucket (§4.5, §7.5).

Лимитер держится только вокруг вызова хендлера (и каждого `__anext__` генератора),
а `emit` идёт вне permit-а (§7.5) — иначе классический deadlock: воркер держит
общий лимит и ждёт место в bounded-стадии, чьи воркеры ждут тот же лимит.
"""

import asyncio
from collections.abc import AsyncGenerator
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from typing import Protocol, runtime_checkable

from staged_pipeline._clock import Clock, MonotonicClock

__all__ = ["Limit", "Limiter"]


@runtime_checkable
class Limiter(Protocol):
    """Ограничитель: контекст, удерживаемый на время вызова хендлера."""

    def acquire(self, weight: int = 1) -> AbstractAsyncContextManager[None]:
        """Вернуть async-контекст, занимающий `weight` единиц лимита."""
        ...


class Limit:
    """Конкурентность и/или частота (token bucket), общий на несколько стадий.

    `concurrency` — не более стольких одновременных вызовов. `rate`/`per` —
    не более `rate` вызовов за `per` секунд (пополнение по `Clock`). Можно задать
    оба; `weighted(n)` — предустановленный вес.
    """

    def __init__(
        self,
        *,
        concurrency: int | None = None,
        rate: float | None = None,
        per: float = 1.0,
        clock: Clock | None = None,
    ) -> None:
        """Создать лимитер.

        Args:
            concurrency: максимум одновременных вызовов (None — без ограничения).
            rate: максимум вызовов за `per` секунд (None — без ограничения частоты).
            per: окно частоты в секундах.
            clock: часы для token bucket (по умолчанию `MonotonicClock`).
        """
        # Конкурентность через счётчик + Condition: позволяет атомарно занять
        # сразу `weight` единиц (семафор дал бы deadlock при частичном захвате).
        self._concurrency: int | None = concurrency
        self._available: int = concurrency if concurrency is not None else 0
        self._cond: asyncio.Condition = asyncio.Condition()
        self._rate: float | None = rate
        self._per: float = per
        self._clock: Clock = clock or MonotonicClock()
        self._capacity: float = float(rate) if rate is not None else 0.0
        self._tokens: float = self._capacity
        self._last: float = self._clock.now()
        self._token_lock: asyncio.Lock = asyncio.Lock()

    @asynccontextmanager
    async def acquire(self, weight: int = 1) -> AsyncGenerator[None]:
        """Контекст, занимающий `weight` единиц конкурентности и токенов."""
        if self._concurrency is not None:
            async with self._cond:
                await self._cond.wait_for(lambda: self._available >= weight)
                self._available -= weight
        if self._rate is not None:
            await self._acquire_tokens(weight)
        try:
            yield
        finally:
            if self._concurrency is not None:
                async with self._cond:
                    self._available += weight
                    self._cond.notify_all()

    def weighted(self, weight: int) -> Limiter:
        """Лимитер с предустановленным весом `weight`."""
        return _Weighted(self, weight)

    async def _acquire_tokens(self, weight: int) -> None:
        """Дождаться `weight` токенов token bucket (пополнение по `Clock`)."""
        async with self._token_lock:
            while True:
                self._refill()
                if self._tokens >= weight:
                    self._tokens -= weight
                    return
                deficit = weight - self._tokens
                wait = deficit * self._per / self._capacity
                await self._clock.sleep(wait)

    def _refill(self) -> None:
        """Пополнить токены по прошедшему времени."""
        now = self._clock.now()
        elapsed = now - self._last
        self._last = now
        if self._capacity:
            self._tokens = min(self._capacity, self._tokens + elapsed * self._capacity / self._per)


class _Weighted:
    """Обёртка `Limiter` с предустановленным весом."""

    def __init__(self, limit: Limit, weight: int) -> None:
        """Запомнить базовый лимит и вес."""
        self._limit: Limit = limit
        self._weight: int = weight

    def acquire(self, weight: int = 1) -> AbstractAsyncContextManager[None]:
        """Занять `self._weight` (аргумент `weight` умножается)."""
        return self._limit.acquire(self._weight * weight)
