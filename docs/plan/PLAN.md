# staged-pipeline v0.1 — план имплементации

> Версия плана 1.0 · 2026-10-24 · к [architecture.md](../architecture.md).
> Прогресс — [PROGRESS.md](PROGRESS.md). Решения по ходу — [DECISIONS.md](DECISIONS.md).
> Шаблон процесса — tallyho (`docs/plan/PLAN.md §0`) и browser_pool, упрощённый под маленькую библиотеку.

## Цель

Опубликовать на PyPI `staged-pipeline` 0.1.0 с API из architecture.md §4. Репозиторий на GitHub,
CI зелёный, релиз по тегу. После этого перевести `checker_account/export/service.py` (§8).

## 0. Протокол итерации

Каждая итерация (вручную или `/loop`) — одна задача. Репозиторий после итерации зелёный.

### 0.1 Шаги

1. **Сориентироваться.** `git status`, `git log --oneline -5`, прочитать [PROGRESS.md](PROGRESS.md)
   (таблица + последние 5 записей журнала) и [DECISIONS.md](DECISIONS.md).
   Незакоммиченные изменения и задача `in_progress` — продолжить её, новую не брать.
2. **Выбрать задачу.** Первая сверху `todo`, у которой все зависимости `done`. `human` и `blocked`
   пропускаются.
3. **Отметить** `in_progress` в PROGRESS.md.
4. **Прочитать** разделы architecture.md из поля «Док» карточки.
5. **Реализовать** вместе с тестами. Во время работы — точечные проверки
   (`uv run pytest tests/test_inbox.py`, `uv run ruff check src/...`).
6. **Гейты** (§0.2) — один раз, когда задача готова.
7. **PROGRESS.md**: статус `done`, хеши коммитов, запись в журнал (дата, что сделано, отклонения).
   Новое архитектурное решение → DECISIONS.md. Отдельный последний коммит
   `T<id>: отметить задачу выполненной`.
8. **Не пушить** до T7.1 (репозитория на GitHub ещё нет). После T7.1 — пушить ветку `impl/v1`, мерж в
   `main` через PR, когда CI зелёный.

### 0.2 Гейты (Definition of Done)

| # | Команда | Что внутри |
|---|---|---|
| G1 | `uv run poe check` | ruff format --check, ruff check, mypy --strict, basedpyright (all), `verifytypes` публичного API, import-linter, deptry, codespell, pytest + покрытие ≥ 90% |
| G2 | `uv run pre-commit run --all-files` | хуки файлов, uv-lock, workflows schema, validate-pyproject |
| G3 | DoD из карточки задачи | конкретные тесты/критерии |

**Тесты быстрые — это правило, а не пожелание:**

- весь набор < 30 с на ноутбуке; `pytest-timeout = 5` на тест;
- время — только через `Clock`; в тестах `FakeClock`, реальные `asyncio.sleep` ≤ 10 мс и только там,
  где проверяется сам event loop;
- hypothesis — профиль `max_examples=100`, без `deadline`; long-профиля нет;
- нет нагрузочных, хаос- и soak-тестов, нет Docker. Бенчмарк оверхеда — скрипт `poe bench`,
  в гейты не входит.

### 0.3 Правила, которые нельзя нарушать

- **Не ослаблять** линтеры, типизацию, покрытие. Подавление точечно и с причиной:
  `# ruff: ignore[rule]  # причина`, `# pyright: ignore[rule]  # причина`.
- Без `typing.Any`, `asyncio.get_event_loop`, `logging.basicConfig`, `unittest.mock.patch` (banned-api).
- Ядро без зависимостей: `dependencies = []`. Extras не импортируют внешних пакетов
  (интеграция tallyho — через `Protocol`, architecture.md §9.3).
- Все исключения библиотеки — подклассы `StagedPipelineError`, кроме исходов `Skip`/`Error`
  (они — `Outcome(Exception)`, архитектурно отдельны). Все модули объявляют `__all__`.
- Комментарии и docstring — на русском, идентификаторы — на английском. Google-style docstring.
- Не менять решения architecture.md молча: сначала запись в DECISIONS.md, потом код.
- Файлы, которые пишет Python, — с `newline="\n"`.

### 0.4 Если не получается

- Задача велика → разбить на `Tx.ya/Tx.yb` (карточки здесь + строки в PROGRESS), закоммитить
  зелёную часть.
- Три честные попытки не помогли или нужен человек → `blocked` с причиной в журнале, код в
  `git stash` с именем задачи, перейти к следующей.
- Дефект в `done`-задаче → новая `Fix-N` в начало очереди, а не правка «заодно».

### 0.5 Когда останавливать `/loop`

Только когда нет `todo` с выполненными зависимостями. Неясность → выбрать вариант, согласованный с
architecture.md, записать в DECISIONS.md как `ACCEPTED (автономно, пересмотреть)` и продолжать.
Задержка между итерациями 60 с.

### 0.6 Коммиты

- Маленькие (до ~200 строк без lock-файла), одно логическое изменение; не смешивать рефакторинг,
  функциональность и документы.
- Сообщение на русском, заголовок `T<id>: <что сделано>` в безличной форме, до 72 символов, тело —
  зачем. Трейлер `Co-Authored-By` по правилам сессии.
- Ветка работы — `impl/v1`; pre-commit запрещает коммиты в `main`.

### 0.7 Окружение

Windows 11, Git Bash / PowerShell, `uv` 0.10, `gh` 2.x. Python 3.13 ставит `uv`
(`uv python install 3.13`). Секреты — только в `.env` (в `.gitignore`), шаблон — `.env.example`.

---

## 1. Порядок

```
T0.1 скелет ─ T0.2 гейты ─┬─ T1.1 ошибки/clock ─┬─ T1.2 политики ───────────┐
                          │                     ├─ T1.3 inbox ──────────────┤
                          │                     └─ T2.1 граф ─ T2.2 учёт ───┤
                          │                                                 ▼
                          │                              T2.3 воркер + run ─ T2.4 ошибки в воркере
                          │                                                 │
                          │      ┌──────────┬──────────┬────────────────────┤
                          │      ▼          ▼          ▼                    ▼
                          │   T3.1 dedup  T3.2 маршр. T3.3 cancel/drain  T3.4 лимиты
                          │      └──────────┴──────────┴─────────┬──────────┘
                          │                                      ▼
                          │                         T4.1 прогресс ─ T4.2 testing
                          │                                      ▼
                          │                         T5.1 tallyho   T5.2 README/примеры
                          └─ T6.1 CI ─ T7.1 GitHub (.env) ─ T7.2 PyPI publisher (human)
                                                                 ▼
                                               T8.1 релиз 0.1.0a1 (human: тег) ─ T9.1 миграция checker
```

## 2. Карточки

### T0.1 — Скелет репозитория

**Док:** —. **Зависит:** —.

- Ветка `main` (переименовать `master`), первый коммит — документы; работа в `impl/v1`.
- `src/staged_pipeline/__init__.py` (`__version__` из `importlib.metadata`), `py.typed`; удалить `main.py`.
- `pyproject.toml`: hatchling, `requires-python = ">=3.13"`, MIT, classifiers 3.13/3.14,
  `Typing :: Typed`, `Framework :: AsyncIO`, urls на `github.com/0cherednoq/staged-pipeline`;
  extras `tallyho = []` (пустой: интеграция без зависимостей, extra — маркер для документации).
- `LICENSE` (MIT), `README.md` (заглушка со статусом), `CHANGELOG.md` (Keep a Changelog),
  `.editorconfig`, `.gitattributes` (`* text=auto eol=lf`), `.gitignore` (+ `.env`, кэши),
  `.python-version` = 3.13, `.env.example`.
- `AGENTS.md` + `CLAUDE.md` (ссылка на AGENTS.md): правила §0.3 коротко.

**DoD:** `uv sync` и `uv build` проходят; `python -c "import staged_pipeline"` из колеса.

### T0.2 — Гейты

**Док:** —. **Зависит:** T0.1.

- Группы `lint` (ruff, mypy, basedpyright, import-linter, deptry, codespell, validate-pyproject),
  `test` (pytest, pytest-asyncio, pytest-timeout, pytest-cov, hypothesis), `dev` (+ poethepoet,
  pre-commit). Версии линтеров закреплены (`==`), как в tallyho.
- Конфиги ruff/mypy/basedpyright — копия строгих настроек tallyho с `target-version = "py313"`.
- import-linter: слои
  `integrations | testing` → `_pipeline` → `_stage | _supervisor` →
  `_accounting | _progress | _policy | _limit | _inbox | _graph` → `_errors | _clock | _types`.
- poe: `fmt`, `lint`, `types`, `api` (`basedpyright --verifytypes staged_pipeline --ignoreexternal`),
  `imports`, `deps`, `spell`, `test`, `check`, `bench`.
- `.pre-commit-config.yaml` — как в tallyho (pre-commit-hooks, uv-lock, codespell,
  check-github-workflows, local-хуки через `uv run --frozen`), pytest на `pre-push`.
- Один тест-заглушка, чтобы pytest и покрытие работали.

**DoD:** G1 и G2 зелёные на скелете.

### T1.1 — Ошибки, исходы, часы

**Док:** §4.3, §4.4, §4.9. **Зависит:** T0.2.

- `_errors.py`: `StagedPipelineError`, `ConfigurationError`, `TopologyError`, `RoutingError`,
  `StageAborted`, `StageClosedError`; `Outcome(Exception)` → `Skip(label)`, `Error(label)`
  (метка валидируется: непустая, ≤ 200 символов).
- `_clock.py`: `Clock`-протокол (`now() -> float`, `sleep(s)`), `MonotonicClock`,
  `FakeClock` (`advance(s)` будит спящих по порядку).

**DoD:** тесты `FakeClock` (порядок пробуждения, `advance(0)`), иерархия исключений.

### T1.2 — Политики

**Док:** §4.4, §4.5. **Зависит:** T1.1.

- `Retry(on=, attempts=, backoff=)`, `Expo(base, cap, jitter=False)`, `Fixed(s)`.
- Классификатор: `classify(exc, retry, skip_on, error_on, attempt) -> Decision`
  (`Retry(delay) | Skip(label) | Error(label) | Fail`), ближайший класс по MRO; значения маппинга —
  строка или `(exc) -> str`. Класс в двух `*_on` или в `retry.on` и `*_on` → `ConfigurationError`.
- `Breaker`-протокол, `ErrorRate(threshold, min_samples, labels=None)`, `NoSuccess(count_labels=())`.

**DoD:** таблица кейсов классификации (parametrize), MRO-приоритет, исчерпание ретраев переходит
в `*_on`, `BaseException` не классифицируется.

### T1.3 — Inbox

**Док:** §7.1. **Зависит:** T1.1.

- `Inbox[T]`: `put_nowait(env)`, `put(env)` (bounded — ждёт), `defer(env, not_before)`,
  `get() -> Envelope | None` (None — закрыт и пуст), `close()`, `cancel() -> list[Envelope]`
  (остаток наружу для учёта как `cancelled`), `len_ready`, `len_deferred`.
- Отложенный элемент становится готовым по `Clock`; `get` ждёт min(not_before) или новый `put`.

**DoD:** тесты на FakeClock: порядок deferred, пробуждение ждущих `get` и `put` при `close/cancel`,
`put` в закрытый → `StageClosedError`.

### T2.1 — Граф

**Док:** §3, §4.2, §7.2, §7.6. **Зависит:** T1.1.

- `StageSpec` (имя, хендлер, workers, buffer, key, политики…), рёбра `a.to(*targets, fallback=)`.
- Вид хендлера и `In/Out` из аннотаций (`typing.get_type_hints`, `inspect.isasyncgenfunction`),
  наличие `ctx`-параметра.
- SCC (Tarjan, итеративно), компоненты + внешние писатели.
- Проверки §7.6 → `TopologyError` с понятным текстом (какая стадия, что не так, как починить).

**DoD:** тесты на каждую проверку §7.6, SCC на DAG / самопетле / цикле из двух стадий.

### T2.2 — Учёт и завершение

**Док:** §3.1, §5, §7.2. **Зависит:** T2.1, T1.3.

- `_accounting.py`: одна функция перехода `transition(stage, env, event)`; счётчики стадии
  (`found, queued, in_flight, deferred, ok, skip, error, cancelled, duplicates`, `labels`, `metrics`,
  `emitted`).
- Компонента: `pending`, `writers`; `check_close()` после каждого декремента; каскад закрытия.

**DoD:** hypothesis-тест: случайная последовательность событий над графом из 3 стадий с циклом —
инвариант `found = queued + in_flight + deferred + done` на каждом шаге; компонента закрывается
ровно тогда, когда `pending == 0 and writers == 0`.

### T2.3 — Воркер, супервизор, `Pipeline.run`

**Док:** §4.1, §4.3, §7.3, §7.4. **Зависит:** T2.2, T1.2.

- `Pipeline`, `Stage`, `pipe.stage(...)`, `seed()` (вход закрывается при старте), `run()`.
- Нормализация выхода: `None | Out | list/tuple | AsyncIterator`, генератор в `aclosing`.
- Супервизор: одна `TaskGroup`, первый сбой хронологически, плоское исключение + `add_note`,
  внешний `CancelledError` как есть; ждущие на `get/put` разбужены.
- Пока без политик: любое исключение хендлера = сбой.

**DoD:** DAG из 3 стадий закрывается; краулер (самопетля) закрывается; «последний элемент
порождает детей» не закрывает раньше; сбой во второй стадии гасит первую, ждущую на bounded `put`;
генератор закрыт после сбоя; повторный `run()` → `RuntimeError`.

### T2.4 — Политики в воркере

**Док:** §4.4, §4.5. **Зависит:** T2.3.

- `retry=` через `Inbox.defer` (слот свободен, `deferred` в pending), `skip_on/error_on`, `Skip/Error`,
  ok-метки через `ctx.label`, `ctx.incr`.
- Логирование исходов (WARNING строка, DEBUG трейсбек), `log_levels=`.
- `abort_if`: проверка после каждого исхода и при закрытии; наружу последнее исходное исключение с
  заметкой или `StageAborted`.
- Проверка «retry + генератор → у получателей есть key».

**DoD:** ретрай не держит воркер (workers=1: второй элемент обработан, пока первый ждёт backoff);
`NoSuccess` при закрытии; `ErrorRate` срабатывает на 20/20; лог через `caplog`.

### T3.1 — Dedup, засев, resume

**Док:** §4.6. **Зависит:** T2.4.

- `key=`, `SeenStore`-протокол + `set`-реализация, `duplicates`; ретрай не проходит dedup.
- `seed()` во время `run()`, `resume(keys, label=)`, `seed_open() -> Feeder` (`put`, `close`,
  `async with`).

**DoD:** дубль из двух папок; resumed в `found/skip`; открытый Feeder держит стадию открытой.

### T3.2 — Маршрутизация и executor

**Док:** §4.2, §4.1. **Зависит:** T2.4.

- `to(a, b)` по `isinstance` к `In`; неоднозначность/непокрытый Union → `TopologyError`;
  рантайм — `RoutingError` или `fallback="drop"` + счётчик `unrouted`.
- `executor="thread"` для sync-хендлеров.

**DoD:** `Page | Image` разводится по двум стадиям; непокрытый член Union ловится при старте.

### T3.3 — `cancel`, `drain`, таймаут

**Док:** §4.7. **Зависит:** T2.4.

- `cancel(reason)`, `drain(timeout)`, `run(timeout=)`; остаток → `cancelled`;
  `report.stopped`; идемпотентность.

**DoD:** drain доделывает in-flight и не берёт новое; drain по таймауту переходит в cancel;
внешняя отмена задачи `run()` пробрасывает `CancelledError` без висящих воркеров.

### T3.4 — Лимиты

**Док:** §4.5, §7.5. **Зависит:** T2.4.

- `Limiter`-протокол, `Limit(concurrency=, rate=, per=)` (семафор + token bucket на `Clock`),
  `.weighted(n)`; permit вокруг вызова/`__anext__`, emit вне permit-а.

**DoD:** общий лимит на две стадии не превышается; тест на отсутствие deadlock: лимит 1 +
bounded(1) downstream под тем же лимитом.

### T4.1 — Прогресс и наблюдение

**Док:** §4.8, §6. **Зависит:** T3.1–T3.4.

- `StageView`, `PipelineView`, `PipelineReport` (frozen dataclass, поля как в §4.8).
- EWMA rate/latency на `Clock`; `expected` по таблице §6 (порог min(20, 5%)); `eta`.
- `pipe.view()`, `pipe.watch(interval)`, `Observer`-протокол + логирующий observer по умолчанию,
  `warn_queued`.

**DoD:** `expected` по каждой строке §6; ETA на FakeClock; `watch()` заканчивается терминальным снимком.

### T4.2 — `staged_pipeline.testing`

**Док:** §4.9. **Зависит:** T4.1.

`run_stage(handler, items, **stage_kwargs) -> StageRun(outputs, view)`, реэкспорт `FakeClock`.

**DoD:** пример из §4.9 работает как тест.

### T5.1 — Интеграция tallyho

**Док:** §9.3. **Зависит:** T4.1.

`staged_pipeline.integrations.tallyho`: `TallyhoItemLike`, `track(pipe, item, every=, drain_timeout=,
weights=)`, `outcome_for(report, partial_if=)` — возвращает простые значения (`("ok", "exported")`),
которые приложение заворачивает в `tallyho.Ok/...` (без импорта tallyho). Имена метрик без U+001F.

**DoD:** тесты с фейковым item: прогресс раз в `every`, `incr` только на выходе, `cancelled()` →
drain.

### T5.2 — README и примеры

**Док:** весь. **Зависит:** T4.2.

README: что это и чем не является, установка, quickstart (3 стадии), таблица «когда Tallyho-этапы,
когда пайплайн», ссылки. `examples/`: `crawler.py` (цикл), `export.py` (fan-out + resume) — оба
запускаются в тестах (`tests/test_examples.py`, < 1 с каждый). `docs/` остаются markdown без сайта.

**DoD:** примеры выполняются в G1.

### T6.1 — CI

**Док:** —. **Зависит:** T0.2.

- `.github/workflows/check.yml`: push в `main`, PR, `workflow_call`. Job `check`: ubuntu + windows,
  py3.13, `uv sync --locked`, `uv run poe check`, pre-commit (SKIP локальных хуков). Job `py314`:
  ubuntu, py3.14, `poe test`. Job `build`: `uv build`, `twine check --strict`, smoke колеса в чистом
  окружении (`uv run --isolated --no-project --with dist/*.whl python -c "import staged_pipeline"`).
  `concurrency` с отменой устаревших прогонов. Бюджет < 5 мин.
- `.github/workflows/release.yml`: тег `v*` → `uses: check.yml` → build + проверка «тег = версия» →
  smoke колеса (ubuntu/windows × 3.13/3.14) → PyPI через Trusted Publishing (environment `pypi`,
  `id-token: write`, без токенов) → GitHub Release с `dist/*` и `--generate-notes`.
- `.github/dependabot.yml` (actions + uv, раз в неделю).

**DoD:** `check-github-workflows` в pre-commit зелёный; `act` не нужен — проверка на GitHub в T7.1.

### T7.1 — GitHub через `.env`

**Док:** —. **Зависит:** T6.1.

- `.env.example` (в git) / `.env` (не в git):
  ```
  # Fine-grained PAT на один репозиторий: Administration RW, Contents RW,
  # Workflows RW, Environments RW, Metadata R.
  GH_TOKEN=
  GITHUB_OWNER=0cherednoq
  GITHUB_REPO=staged-pipeline
  GITHUB_VISIBILITY=public
  ```
- `scripts/github_bootstrap.py` (stdlib, читает `.env` сам, без python-dotenv; вызывает `gh` с
  `GH_TOKEN` в окружении процесса, токен не печатает) — `poe github`. Идемпотентно:
  1. `gh repo create` (если нет), `git remote add origin`, push `main` и `impl/v1`;
  2. описание, topics (`asyncio`, `pipeline`, `dataflow`, `seda`), отключить wiki/projects;
  3. environment `pypi` с required reviewer = владелец (ручное подтверждение публикации);
  4. защита `main`: обязательный статус `check`, запрет force-push.
- Проверка: CI на `impl/v1` зелёный на GitHub.

**DoD:** повторный `poe github` ничего не меняет; `gh run list` — зелёный `check`.

### T7.2 — PyPI Trusted Publisher — `human`

На pypi.org → Publishing → «Add a new pending publisher»: project `staged-pipeline`, owner
`0cherednoq`, repo `staged-pipeline`, workflow `release.yml`, environment `pypi`. Токен PyPI не нужен
ни в `.env`, ни в секретах GitHub.

### T8.1 — Релиз 0.1.0a1 — `human` на последнем шаге

**Зависит:** T5.1, T5.2, T7.1, T7.2.

Агент: версия `0.1.0a1` в pyproject, CHANGELOG, PR `impl/v1 → main`, CI зелёный, мерж.
Человек: `git tag v0.1.0a1 && git push origin v0.1.0a1`, подтверждение environment `pypi` в Actions.
Проверка: `uv run --with staged-pipeline==0.1.0a1 --no-project python -c "import staged_pipeline"`.

### T9.1 — Миграция checker_account (другой репозиторий)

**Док:** §8. **Зависит:** T8.1.

`A:\work\stardust\scripts\checker_account`: зависимость `staged-pipeline`, переписать `_collect_mail`,
`ExportMeter`/`render_card` на `PipelineView`, `export_one` — `track()`. Тесты проекта зелёные,
живой прогон одного ящика — `human`.
