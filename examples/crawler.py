"""Пример: краулер на самопетле (циклический граф).

Стадия порождает «детей» обратно в себя; пайплайн завершается по quiescence,
когда больше нечего обрабатывать. Запуск: `python examples/crawler.py`.
"""

import asyncio

from staged_pipeline import Pipeline

MAX_DEPTH = 2


async def crawl(depth: int) -> list[int]:
    """Вернуть «ссылки» на следующий уровень, пока не достигнута глубина."""
    if depth < MAX_DEPTH:
        return [depth + 1, depth + 1]
    return []


async def main() -> int:
    """Прогнать краулер от корня и вернуть число посещённых узлов."""
    pipe = Pipeline("crawler")
    stage = pipe.stage("crawl", crawl)
    stage.to(stage)  # самопетля: выход возвращается на вход
    stage.seed([0])
    report = await pipe.run()
    return report.stages["crawl"].ok


if __name__ == "__main__":
    print("посещено узлов:", asyncio.run(main()))
