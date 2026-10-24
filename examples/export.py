"""Пример: экспорт почты (fan-out + dedup + resume) — DAG из трёх стадий.

`list` сканирует папку в треды, `resolve` разворачивает тред во вложения (fan-out),
`media` скачивает. Ключи дают дедупликацию, `resume` помечает уже сделанные треды.
Запуск: `python examples/export.py`.
"""

import asyncio
from collections.abc import AsyncIterator
from dataclasses import dataclass

from staged_pipeline import Pipeline, PipelineReport


@dataclass(frozen=True)
class Spec:
    """Вложение, которое нужно скачать."""

    thread: str
    attachment: str


async def scan(folder: str) -> AsyncIterator[str]:
    """Потоковый fan-out: выдать треды папки по одному."""
    for i in range(3):
        yield f"{folder}-t{i}"


async def resolve(thread: str) -> list[Spec]:
    """Атомарный fan-out: тред → список вложений."""
    return [Spec(thread, f"{thread}-a{j}") for j in range(2)]


async def download(spec: Spec) -> None:
    """Терминальная стадия: «скачать» вложение (здесь — no-op)."""
    del spec


def _thread_key(thread: str) -> str:
    return thread


def _spec_key(spec: Spec) -> str:
    return spec.attachment


async def main() -> PipelineReport:
    """Собрать и прогнать пайплайн экспорта; вернуть отчёт."""
    pipe = Pipeline("export")
    listing = pipe.stage("list", scan)
    threads = pipe.stage("resolve", resolve, key=_thread_key)
    media = pipe.stage("media", download, key=_spec_key)
    listing.to(threads)
    threads.to(media)
    listing.seed(["inbox"])
    threads.resume(["inbox-t0"], label="resumed")  # этот тред уже обработан ранее
    return await pipe.run()


if __name__ == "__main__":
    result = asyncio.run(main())
    for name, view in result.stages.items():
        print(f"{name}: ok={view.ok} skip={view.skip} found={view.found}")
