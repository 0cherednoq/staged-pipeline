"""staged-pipeline — декларативный конвейер стадий поверх asyncio.

Публичный API появляется по мере реализации (см. docs/plan/PLAN.md). На этапе
скелета пакет экспортирует только версию.
"""

from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version("staged-pipeline")
except PackageNotFoundError:  # pragma: no cover - пакет не установлен (запуск из исходников)
    __version__ = "0.0.0.dev0"

__all__ = ["__version__"]
