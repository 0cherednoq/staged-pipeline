"""Примеры из examples/ исполняются как тесты (T5.2, < 1 с каждый)."""

import sys
from pathlib import Path

_EXAMPLES = Path(__file__).resolve().parent.parent / "examples"
if str(_EXAMPLES) not in sys.path:
    sys.path.insert(0, str(_EXAMPLES))

import crawler  # noqa: E402
import export  # noqa: E402


async def test_crawler_example() -> None:
    visited = await crawler.main()
    # 1 (d0) + 2 (d1) + 4 (d2) = 7 узлов.
    assert visited == 7


async def test_export_example() -> None:
    report = await export.main()
    # 3 треда найдено; inbox-t0 помечен resumed (skip), t1/t2 обработаны.
    assert report.stages["resolve"].found == 3
    assert report.stages["resolve"].skip == 1
    assert report.stages["resolve"].ok == 2
    # t1/t2 × 2 вложения = 4.
    assert report.stages["media"].ok == 4
