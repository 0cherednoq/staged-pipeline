# staged-pipeline

> **Статус: pre-release (0.1.0a).** API описан в [docs/architecture.md](docs/architecture.md),
> реализация — по [docs/plan/PLAN.md](docs/plan/PLAN.md). До релиза `0.1.0` возможны изменения.

Декларативный конвейер стадий поверх `asyncio` внутри одного процесса: пул воркеров на стадию,
дедупликация, ретраи с backoff, предохранители, лимиты и живой прогресс. Режим завершения выводится
из графа (DAG закрывается по писателям, цикл — по quiescence), поэтому «выбрать неправильный режим и
повиснуть» нельзя.

## Чем это является

- слой внутри одного процесса и одного event loop: стадии с воркерами, маршрутизацией по типу,
  политиками исходов (`retry`/`skip_on`/`error_on`), лимитами и дедупликацией;
- источник честного прогресса: `found / queued / in_flight / deferred / ok / skip / error /
  cancelled / duplicates`, `expected` и `eta`;
- одноразовый граф, проверяемый до старта (ошибка топологии не запускает ничего).

## Чем это не является

- не durable и не распределённая система — всё в одном процессе;
- не владеет хранилищем: устойчивость между прогонами даёт БД домена (resume засевом);
- не замена очереди задач (Tallyho и подобных) — это слой внутри одного Item.

## Установка

```bash
uv add staged-pipeline            # ядро, без зависимостей
uv add "staged-pipeline[tallyho]" # + маркер интеграции с Tallyho
```

Требуется Python 3.13+.

## Quickstart

```python
import asyncio
from staged_pipeline import Pipeline


async def scan(folder: str):          # async-генератор: потоковый fan-out
    for i in range(3):
        yield f"{folder}/{i}"


async def resolve(path: str) -> list[str]:   # атомарный fan-out
    return [f"{path}.a", f"{path}.b"]


async def download(url: str) -> None:        # терминальная стадия
    ...


async def main() -> None:
    pipe = Pipeline("export")
    listing = pipe.stage("list", scan, workers=2)
    resolving = pipe.stage("resolve", resolve, workers=8, key=lambda p: p)
    media = pipe.stage("media", download, workers=4, key=lambda u: u)

    listing.to(resolving)
    resolving.to(media)
    listing.seed(["inbox"])

    report = await pipe.run()
    print(report.stages["media"].ok)


asyncio.run(main())
```

Больше примеров — в [examples/](examples/): `crawler.py` (цикл/самопетля) и `export.py`
(fan-out + dedup + resume). Оба исполняются в тестах.

## Когда этапы Tallyho, а когда пайплайн

| Критерий | Этапы Tallyho (`sub_batch` + `fed_by`) | Пайплайн внутри Item |
|---|---|---|
| Элемент должен пережить падение воркера сам по себе | ✔ | ✘ (resume — через БД домена) |
| Элементы дешёвые (≪ 100 мс) и их 10⁴–10⁶ на единицу | дорого: строка + коммит на элемент | ✔ |
| Общая сессия/лиз/соединение на единицу | ✘ (лиз на задачу) | ✔ |
| Масштаб на несколько машин | ✔ | ✘ |
| Нужен точный итог по каждому элементу в БД | ✔ | домен пишет сам |

Мост прогресса и остановки к Tallyho — `staged_pipeline.integrations.tallyho` (`track`,
`outcome_for`), без зависимости от `tallyho`. Подробности — [architecture.md §9](docs/architecture.md).

## Документация

- [Архитектура и API](docs/architecture.md)
- [Prior art / ресёрч](docs/research.md)
- [План реализации](docs/plan/PLAN.md) · [прогресс](docs/plan/PROGRESS.md) ·
  [решения](docs/plan/DECISIONS.md)

## Лицензия

[MIT](LICENSE).
