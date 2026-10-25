# Руководство: staged-pipeline по задачам

Практичные примеры по возрастанию сложности. Полный API — в
[architecture.md](architecture.md), это «как сделать X».

Каждый пример самодостаточен: `import asyncio`, затем `asyncio.run(main())`.

- [1. Линейный конвейер из трёх стадий](#1-линейный-конвейер-из-трёх-стадий)
- [2. Fan-out: один элемент → много](#2-fan-out-один-элемент--много)
- [3. Маршрутизация по типу выхода](#3-маршрутизация-по-типу-выхода)
- [4. Циклы и краулеры](#4-циклы-и-краулеры)
- [5. Политики ошибок: retry, skip, error](#5-политики-ошибок-retry-skip-error)
- [6. Предохранители: abort_if](#6-предохранители-abort_if)
- [7. Дедупликация и resume](#7-дедупликация-и-resume)
- [8. Открытый вход: Feeder](#8-открытый-вход-feeder)
- [9. Лимиты: concurrency и rate](#9-лимиты-concurrency-и-rate)
- [10. Прогресс, ETA и наблюдение](#10-прогресс-eta-и-наблюдение)
- [11. Синхронные хендлеры: executor="thread"](#11-синхронные-хендлеры-executorthread)
- [12. Остановка: cancel, drain, timeout](#12-остановка-cancel-drain-timeout)
- [13. Тестирование стадий](#13-тестирование-стадий)
- [14. Интеграция с Tallyho](#14-интеграция-с-tallyho)

---

## 1. Линейный конвейер из трёх стадий

Базовый случай: источник → преобразование → приёмник. У каждой стадии свой пул воркеров;
завершение выводится из графа — ничего не нужно закрывать вручную.

```python
import asyncio
from staged_pipeline import Pipeline


async def scan(folder: str):                 # async-генератор: потоковый выход
    for i in range(3):
        yield f"{folder}/{i}"


async def resolve(path: str) -> str:         # один вход → один выход
    return path.upper()


async def store(name: str) -> None:          # терминальная стадия (выход None)
    print("stored", name)


async def main() -> None:
    pipe = Pipeline("etl")
    s1 = pipe.stage("scan", scan, workers=2)
    s2 = pipe.stage("resolve", resolve, workers=8)
    s3 = pipe.stage("store", store, workers=4)

    s1.to(s2)
    s2.to(s3)
    s1.seed(["inbox"])                       # вход источника; закроется при старте

    report = await pipe.run()
    print(report.stages["store"].ok)         # 3


asyncio.run(main())
```

**Вид хендлера выводится из сигнатуры:** `async def … yield` → потоковый fan-out; `async def …
-> T` → один элемент; `-> None` → терминальная стадия; `-> list[T]`/`-> tuple[...]` → атомарный
fan-out (см. §2).

---

## 2. Fan-out: один элемент → много

Два способа размножить элементы. **Атомарный** (`list`/`tuple`) — всё уходит после успеха, при
исключении не уходит ничего. **Потоковый** (генератор) — каждый `yield` уходит сразу.

```python
async def resolve_thread(thread_id: str) -> list[str]:   # атомарный fan-out
    return [f"{thread_id}.a", f"{thread_id}.b"]


async def pages(folder: str):                            # потоковый fan-out
    page = 0
    while page < 10:
        yield f"{folder}?page={page}"
        page += 1
```

Разница важна для ретраев: потоковый хендлер при повторе выдаст первые элементы **ещё раз**,
поэтому стадия с `retry=` и генератором требует `key=` у получателей (иначе `TopologyError` при
старте). Для `list` такого требования нет — он атомарен.

---

## 3. Маршрутизация по типу выхода

Если у стадии несколько рёбер, элемент уходит той стадии, чей входной тип ему подходит
(`isinstance`). Неоднозначность или непокрытый член `Union` — `TopologyError` при старте.

```python
from dataclasses import dataclass


@dataclass
class Page: url: str

@dataclass
class Image: url: str


async def crawl(url: str) -> Page | Image:
    return Image(url) if url.endswith(".png") else Page(url)


async def handle_page(p: Page) -> None: ...
async def handle_image(i: Image) -> None: ...


async def main() -> None:
    pipe = Pipeline("router")
    c = pipe.stage("crawl", crawl)
    c.to(pipe.stage("pages", handle_page), pipe.stage("images", handle_image))
    c.seed(["a.html", "b.png"])
    await pipe.run()
```

Если выход не подошёл никому — в рантайме `RoutingError` (сбой), либо, при
`c.to(..., fallback="drop")`, элемент тихо отбрасывается и считается в метрике `unrouted`.

---

## 4. Циклы и краулеры

Стадия может отправлять выход **обратно себе** (`stage.to(stage)`) или в цикл из нескольких
стадий. Пайплайн завершится по quiescence — когда в компоненте не осталось работы. `buffer`
внутри цикла запрещён (источник deadlock) — `TopologyError`.

```python
async def crawl(depth: int) -> list[int]:
    if depth < 3:
        return [depth + 1, depth + 1]       # «ссылки» на следующий уровень
    return []


async def main() -> None:
    pipe = Pipeline("crawler")
    stage = pipe.stage("crawl", crawl, key=lambda d: d)   # key: не ходить по одному дважды
    stage.to(stage)
    stage.seed([0])
    report = await pipe.run()
    print(report.stages["crawl"].ok)
```

---

## 5. Политики ошибок: retry, skip, error

Судьба исключения объявляется на стадии. Порядок разбора — ближайший класс по MRO:

1. `raise Skip("label")` / `raise Error("label")` — явный исход;
2. класс в `retry.on` и попытки есть → отложенный ретрай (слот воркера свободен);
3. `skip_on` / `error_on` по классу → `skip`/`error`;
4. иначе — **сбой пайплайна** (fail-fast: тихо проглатывать нельзя).

```python
from staged_pipeline import Pipeline, Retry, Expo, Skip, Error


class RateLimited(Exception): ...
class NotFound(Exception): ...


async def fetch(url: str, ctx) -> bytes:
    data = await download(url)               # может бросить RateLimited/NotFound/прочее
    if not data:
        raise Skip("empty")                  # осознанный пропуск
    ctx.label("fetched")                     # метка успешного исхода (по умолч. "ok")
    ctx.incr("bytes", len(data))             # доменная метрика
    return data


pipe.stage(
    "fetch", fetch, workers=16,
    retry=Retry(on=RateLimited, attempts=3, backoff=Expo(0.5, cap=10, jitter=True)),
    skip_on={NotFound: "deleted"},           # класс → метка (или функция exc -> метка)
    error_on={Exception: "failed"},          # широкий класс: ловит всё прочее как error
)
```

Обработанный исход логируется (WARNING строкой, DEBUG — трейсбек); уровни настраиваются
`log_levels={"skip": logging.INFO, ...}`. Исчерпанный ретрай уходит в `error_on` по ближайшему
классу (поэтому ретраят узкий класс, а в `error_on` кладут широкий).

---

## 6. Предохранители: abort_if

Останавливают стадию, когда «что-то системно не так». Наружу уходит **последнее исходное
исключение** стадии (с заметкой), чтобы вызывающий код/Tallyho увидели причину.

```python
from staged_pipeline import ErrorRate, NoSuccess

pipe.stage(
    "resolve", resolve,
    error_on={Exception: "failed"},
    abort_if=ErrorRate(threshold=0.9, min_samples=20),   # во время работы
)

pipe.stage(
    "resolve", resolve,
    error_on={Exception: "failed"},
    abort_if=NoSuccess(count_labels={"resumed"}),        # при закрытии: ok==0 и error>0
)
```

`NoSuccess(count_labels={"resumed"})` не считает resume-пропуски за «ничего не сделали» — прогон
из одних resumed не роняет стадию. Можно список: `abort_if=[ErrorRate(...), NoSuccess()]`.

---

## 7. Дедупликация и resume

`key=` даёт at-most-once в пределах прогона: повтор ключа — `duplicate`, не обрабатывается
дважды. Ретрай dedup **не** проходит повторно (ключ помечен принятым при приёме). `resume(keys)`
помечает уже сделанное: встреченный ключ идёт в `found` и `skip(label)` — прогресс показывает
полный объём, а не остаток.

```python
async def main() -> None:
    pipe = Pipeline("export")
    threads = pipe.stage("resolve", resolve, key=lambda t: t.thread_id)
    media = pipe.stage("media", download, key=lambda s: s.attachment_id)
    threads.to(media)

    threads.seed(await scan_all())
    threads.resume(await repo.resolved_threads(job), label="resumed")  # уже записанные
    media.seed(await repo.pending_attachments(job))                    # досев недокачанного
    await pipe.run()
```

Хранилище ключей — протокол `SeenStore` (`add(key) -> bool`, `__contains__`); по умолчанию — `set`.

---

## 8. Открытый вход: Feeder

Когда элементы приходят извне во время прогона (очередь, сокет), `seed_open()` даёт `Feeder`:
стадия не закроется, пока `Feeder` открыт.

```python
async def main() -> None:
    pipe = Pipeline("live")
    stage = pipe.stage("handle", handle)
    feed = stage.seed_open()

    async with asyncio.TaskGroup() as tg:
        run = tg.create_task(pipe.run())
        async with feed:                     # закроет вход при выходе из блока
            async for event in external_source():
                await feed.put(event)
        await run
```

---

## 9. Лимиты: concurrency и rate

Общий лимитер разделяется между стадиями: суммарная нагрузка на внешний ресурс (API, сессию) под
контролем. Permit держится только вокруг вызова хендлера — на `emit` он отпускается (иначе
deadlock).

```python
from staged_pipeline import Limit

api = Limit(concurrency=24, rate=40, per=1.0)    # ≤24 разом И ≤40 вызовов/с

pipe.stage("resolve", resolve, workers=32, limits=[api])
pipe.stage("media", download, workers=32, limits=[api.weighted(2)])   # вес 2 на тяжёлой стадии
```

> ⚠️ Не вешайте один общий лимит на две стадии, если одна быстрая, а другая долгая: долгая съест
> все слоты и быстрая встанет. Это осознанный выбор — у каждой стадии обычно свой `workers=`.

---

## 10. Прогресс, ETA и наблюдение

Снимок дешёвый и синхронный. `expected`/`eta` считаются по данным писателей (см. §6 architecture).

```python
view = pipe.view()                           # во время/после прогона
s = view.stages["media"]
print(s.found, s.queued, s.in_flight, s.ok, s.error, s.expected, s.eta)

# Поток снимков раз в секунду, заканчивается терминальным снимком:
async for snapshot in pipe.watch(interval=1.0):
    render(snapshot)

report = await pipe.run()                    # PipelineReport = последний снимок + duration + stopped
```

Для событий — `Observer` (все методы опциональны): `stage_closed`, `item_finished`,
`breaker_tripped`, `queued_warning`. Передаётся в `Pipeline(..., observer=...)`; по умолчанию
логирующий. Исключения в наблюдателе изолированы и не роняют прогон.

---

## 11. Синхронные хендлеры: executor="thread"

Блокирующий код (CPU, sync-библиотека) можно запускать в пуле потоков.

```python
def parse(blob: bytes) -> dict:              # обычный def, не async
    return heavy_sync_parse(blob)

pipe.stage("parse", parse, executor="thread")
```

> ⚠️ `asyncio.to_thread` использует общий пул потоков интерпретатора, поэтому реальная
> параллельность ограничена его размером, а не `workers=`. И такой хендлер **нельзя прервать**:
> `cancel()`/таймаут отменят ожидание, но поток доработает. Для долгих задач это учитывайте.

---

## 12. Остановка: cancel, drain, timeout

```python
pipe.cancel("reason")            # немедленно: воркеры отменены, остаток → cancelled
await pipe.drain(timeout=30)     # мягко: входы закрыты, in-flight доигрывают, остаток → cancelled
report = await pipe.run(timeout=1800)         # по таймауту → как cancel
```

Остановка по просьбе — **не сбой**: `run()` вернёт отчёт с `report.stopped == "cancel"|"drain"`.
Внешняя отмена задачи `run()` пробрасывает `CancelledError` как есть (воркеры погашены).

---

## 13. Тестирование стадий

`staged_pipeline.testing.run_stage` прогоняет одну стадию и собирает её выход. `FakeClock`
даёт детерминированное время.

```python
from staged_pipeline.testing import run_stage, FakeClock


async def test_resolve():
    out = await run_stage(resolve, [ref1, ref2], key=lambda r: r.id, skip_on={NotFound: "gone"})
    assert sorted(out.outputs) == [...]
    assert out.view.ok == 1
    assert out.view.skip == 1
```

> Для сценариев с реальными ретраями используйте настоящие часы: `run_stage` доигрывает до конца,
> а отложенный ретрай на `FakeClock` без `advance()` не наступит.

---

## 14. Интеграция с Tallyho

Мост прогресса и остановки — `staged_pipeline.integrations.tallyho`, **без** зависимости от
`tallyho` (через структурный `Protocol`).

```python
from staged_pipeline.integrations.tallyho import track, outcome_for

async def export_one(item) -> tuple[str, str]:
    pipe = build_pipeline()
    async with track(pipe, item, every=2.0, drain_timeout=30, weights={"media": 3}):
        report = await pipe.run()
    return outcome_for(report, partial_if=lambda r: r.stages["media"].error > 0)
    # ("ok", "exported") | ("ok", "partial") — приложение заворачивает в tallyho.Ok(...)
```

`track` раз в `every` шлёт `item.progress(done, total)` (взвешенные по стадиям), при
`item.cancelled()` переводит пайплайн в `drain`, а на выходе выгружает итоговые счётчики в
`item.incr`.
