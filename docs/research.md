# Prior art: in-process staged dataflow — как сделано и где грабли

> Источник: ресёрч 2026-10-24. Пометка **[память]** — пункт по знанию документации, не перепроверен
> по первоисточнику; перед цитированием наружу проверить.
> Вывод для API — в конце и в [architecture.md](architecture.md) §2.

## Сводная таблица

| Инструмент | Модель | Что взять | Главная грабля |
|---|---|---|---|
| .NET TPL Dataflow | блоки + `LinkTo(PropagateCompletion)` | per-block `MaxDegreeOfParallelism`, `BoundedCapacity`, unbounded по умолчанию | fault идёт только вниз; fan-in закрывается первым писателем; predicate без матча вешает блок |
| .NET Channels | канал + ручной `Complete` | `TryComplete`, `ReadAllAsync` | completion полностью ручной, Drop-режимы теряют молча |
| Elixir Broadway | producer → processors → batchers → ack | `handle_failed`, graceful drain, метрики из коробки | failed-сообщение без лога; нет retry (только redelivery брокера) |
| GenStage / Flow | demand-driven | per-stage concurrency | demand-модель плохо ложится на циклы [память] |
| Pekko/Akka Streams | `Source ~> Flow ~> Sink`, сквозной backpressure | supervision `Stop/Resume/Restart`, `mapAsyncUnordered`, `Source.queue` | Resume/Restart выкидывают элемент; не все операторы уважают decider; GraphDSL/materialized values сложны |
| asyncio `Queue.shutdown` (3.13) | очередь с закрытием | `QueueShutDown` будит ждущих | `immediate=True` молча теряет элементы; доки `join()` правили в 3.14.1 |
| trio/anyio memory channels | `send.clone()` | **канал закрыт, когда закрыты все клоны** — эталон fan-in | забытый оригинал = вечное ожидание; буфер 0 по умолчанию → deadlock |
| pypeln | `pl.task.map(f, stage, workers, maxsize)` | ближе всех по форме API | фактически не сопровождается; теряет исключения, виснет при ошибке стадии |
| aiostream | pull-комбинаторы | — | нет очередей стадий, метрик, retry; сквозной backpressure |
| aiometer | `amap(max_at_once, max_per_second)` | **лимит = concurrency + rate в одном объекте** | стадий нет |
| Crawlee | RequestQueue + Router по label | dedup `unique_key`, неизвестный label → ошибка | `is_finished` проверяется только при нуле задач; `keepAlive` |
| Scrapy | scheduler + dupefilter + `spider_idle` | resume через сохранённый seen-set (`JOBDIR`); retry возвращает в scheduler, не держит слот | retry обязан обходить dedup (`dont_filter=True`) |
| Go pipelines / errgroup / conc | каналы, close-by-sender, ctx | первая ошибка отменяет ctx; conc ловит panic | утечка горутин без `select <-done`; `errgroup.Go` из своей же горутины при лимите → deadlock |
| Kotlin Flow | `buffer`, `flatMapMerge(16)` | явный разрыв backpressure | неявный default 16; `catch` ловит только upstream |
| Rust `buffer_unordered` | futures опрашиваются только при pull | — | **futurelock**: буферизованная работа стоит, пока тело цикла ждёт; permit + ожидание = deadlock |
| Новые Python (anyiostream, flowlet, pipestage, AntFlow) | стадии с workers/buffer | — | молодые, малоиспользуемые — ниша свободна |

## Детали по инструментам

### TPL Dataflow (.NET)

- Fault распространяется только downstream; upstream-блоки «утекают», bounded-голова откладывает
  сообщения бесконечно, `SendAsync` продюсера виснет. Отсюда обёртки с гарантией «pipeline completed ⇒
  все блоки completed». https://github.com/theodorzoulias/SimpleTplDataflowPipelines
- Каждый блок оборачивает чужую ошибку → вложенные `AggregateException`.
  https://devblogs.microsoft.com/dotnet/exception-handling-in-tpl-dataflow-networks/
- `PropagateCompletion` с несколькими источниками: первый завершившийся закрывает target, остальные
  получают отказ (`Post → false`). Правильно — `WhenAll(sources).ContinueWith(target.Complete)`.
  https://blog.stephencleary.com/2012/09/introduction-to-dataflow-part-1.html
- `LinkTo(predicate)`: сообщение без подходящего линка навсегда застревает в выходном буфере, блок не
  завершается. Официальный совет — `NullTarget` последним, т.е. молчаливая потеря. [память]
- `Post` в bounded-блок молча возвращает `false`; bounded `ActionBlock(capacity=1)` — deadlock.
  https://devblogs.microsoft.com/premier-developer/dissecting-the-actionblock-a-short-story-about-a-nasty-deadlock/
- Faulted-блок выбрасывает буфер; `EnsureOrdered` + DOP>1 → head-of-line blocking. [память]

### Broadway / GenStage

- Исключение в `handle_message` помечает сообщение failed без лога — тихие потери у тех, кто не
  реализовал `handle_failed`. https://hexdocs.pm/broadway/Broadway.Message.html
- Встроенного retry нет; повтор = redelivery брокера → побочные эффекты должны быть идемпотентны.
- Broadway появился, потому что каждая команда заново писала ack/drain/rate-limit/метрики поверх
  GenStage — тот же мотив, что у нашей библиотеки.

### Pekko/Akka Streams

- `Resume`/`Restart` выбрасывают упавший элемент; `Restart` ещё и сбрасывает состояние стадии;
  default — `Stop`. https://pekko.apache.org/docs/pekko/current/stream/stream-error.html
- Supervision поддерживают не все операторы. [память]
- Сложность GraphDSL и materialized values — частая жалоба.

### Python

- `asyncio.Queue.shutdown(immediate=True)` очищает очередь без колбэка — элементы теряются без учёта.
  https://docs.python.org/3/library/asyncio-queue.html
- trio/anyio: закрытие по последнему клону — правильная модель fan-in, но ручная (забыл закрыть
  оригинал → вечное ожидание). https://trio.readthedocs.io/en/stable/reference-core.html
- `TaskGroup` всегда отдаёт `ExceptionGroup`, даже для одной ошибки — частая жалоба. [память]
- pypeln: 0.4.9, без релизов давно, issues про потерю исключений и зависания. https://github.com/cgarciae/pypeln
- Crawlee: label без handler-а → ошибка (правильно), завершение через `is_finished` при нуле задач.
  https://crawlee.dev/python/api/class/AutoscaledPool
- Scrapy: `spider_idle` = scheduler пуст и нет загрузок; retry → `dont_filter=True`. [память]

### Go

- https://go.dev/blog/pipelines — канал закрывает отправитель; при нескольких — координатор после
  `wg.Wait()`; каждая отправка обязана `select` с `done`, иначе утечка горутин.
- `errgroup.SetLimit` + `Go` изнутри группы → deadlock (рекурсивный краулер); `Wait` отдаёт только
  первую ошибку. https://pkg.go.dev/golang.org/x/sync/errgroup
- `sourcegraph/conc` — ответ на «легко забыть `wg.Done`/`recover`». https://github.com/sourcegraph/conc

### Rust

- `buffer_unordered`: работа внутри буфера продвигается только при опросе стрима → «futurelock»,
  deadlock при удержании permit. https://rust-lang.github.io/wg-async/vision/submitted_stories/status_quo/barbara_battles_buffered_streams.html ,
  https://github.com/rust-lang/futures-rs/issues/2387 , https://without.boats/blog/poll-progress/

### Завершение в циклических графах

Краулеры (Scrapy `spider_idle`, Crawlee `is_finished`, Colly `WaitGroup`) используют глобальный
счётчик pending/in-flight. Корректная схема (Dijkstra–Scholten в одном потоке): **инкремент при
`emit` ребёнка до `put`, декремент родителя — только после выхода его handler-а**. Инкремент на
`get` или ранний декремент дают преждевременное завершение (известные жалобы на Colly [память]).
В одном event loop проверка «счётчик == 0» атомарна, если между `put` и инкрементом нет `await`.

## Уроки → правила API

| # | Урок | Откуда |
|---|---|---|
| 1 | Fail-fast гасит все стадии, и upstream тоже; будит всех ждущих на put/get. `await run()` вернулся ⇒ все воркеры завершены | TPL, Go |
| 2 | Fan-in закрывается, когда закрыты **все** писатели; закрытие идемпотентно | TPL, trio, Channels |
| 3 | Цикл → quiescence: инкремент при emit до put, декремент после handler-а родителя | Scrapy, Crawlee, Colly |
| 4 | Ничего не терять молча: нет маршрута, отмена, переполнение → ошибка или явный счётчик | TPL, Akka, Queue.shutdown |
| 5 | Ошибка наружу плоская, с контекстом стадии и элемента | TPL, TaskGroup |
| 6 | Обработанная ошибка оставляет след: лог по умолчанию + счётчик + метка | Broadway |
| 7 | Retry не держит слот воркера, но элемент остаётся «живым» для завершения | Scrapy |
| 8 | Retry обходит dedup; ключ помечается seen при приёме, а не при успехе | Scrapy |
| 9 | Resume = засев ключей, не элементов | Scrapy JOBDIR |
| 10 | Политики элемента и политики жизни воркера не смешивать; restart-со-сбросом не нужен | Akka |
| 11 | Воркеры — свои `asyncio.Task`, а не ленивые генераторы | Rust futurelock |
| 12 | Bounded-буфер внутри цикла запрещён (самоожидание) | Go, Akka |
| 13 | Лимитер держится только вокруг handler-а, не во время ожидания места внизу | futures-rs#2387, errgroup |
| 14 | `drain()` и `cancel()` — два явных режима, идемпотентны, конечное время | Broadway, Go |
| 15 | Порядок не гарантирован при workers>1 — в контракте | TPL, anyiostream |
| 16 | Переходы состояния элемента считаются в одной точке; инвариант счётчиков в тестах | — |
| 17 | Запись в закрытую стадию — исключение, а не `False` | TPL `Post` |
| 18 | Граф декларативный, проверяется при старте; никаких materialized values | Akka |

Расхождение с ресёрчем: он советует bounded по умолчанию. Мы оставляем **unbounded по умолчанию**
(как TPL): у нас между стадиями ходят ссылки (id, spec), а bounded — единственный источник deadlock-ов
(правило 12, 13). Риск памяти закрывается метрикой `queued` и `warn_queued`.
