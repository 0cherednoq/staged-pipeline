# staged-pipeline v0.1 — прогресс

> Протокол обновления — [PLAN.md §0](PLAN.md#0-протокол-итерации). Файл правится в каждой итерации и
> коммитится вместе с задачей.
> Статусы: `todo` · `in_progress` · `done` · `blocked` (причина в журнале) · `human` (нужен человек).

## Текущее состояние

* **Ветка:** `impl/v1`
* **Текущая задача:** T4.1 (прогресс и наблюдение)
* **Последний зелёный коммит:** `c81bd6d` (T3.4)
* **Среда:** оффлайн (нет DNS/PyPI), тулчейн ставится из кэша uv — см. [D-009](DECISIONS.md).

## Задачи

| ID | Задача | Зависит | Статус | Коммиты |
|---|---|---|---|---|
| T0.1 | Скелет репозитория | — | done | `3945150` |
| T0.2 | Гейты: линтеры, типы, import-linter, pytest, pre-commit | T0.1 | done | `2c9fead` |
| T1.1 | Ошибки, исходы `Skip/Error`, `Clock`/`FakeClock` | T0.2 | done | `2b5125d` |
| T1.2 | Политики: `Retry`, классификация, `ErrorRate`/`NoSuccess` | T1.1 | done | `1677e68` |
| T1.3 | `Inbox`: готовые + отложенные, close/cancel с остатком | T1.1 | done | `0a6a988` |
| T2.1 | Граф: стадии, рёбра, типы из аннотаций, SCC, проверки | T1.1 | done | `472f445` |
| T2.2 | Учёт: функция перехода, счётчики, закрытие компонент | T2.1, T1.3 | done | `2066448` |
| T2.3 | Воркер, супервизор, `Pipeline.run` | T2.2, T1.2 | done | `5f5f72b` |
| T2.4 | Политики в воркере, логирование, `abort_if` | T2.3 | done | `30a63d6` |
| T3.1 | Dedup, `seed`, `resume`, `seed_open` | T2.4 | done | `3813556` |
| T3.2 | Маршрутизация по типу, `executor="thread"` | T2.4 | done | `b66360b` |
| T3.3 | `cancel`, `drain`, таймаут | T2.4 | done | `03987b0` |
| T3.4 | Лимиты | T2.4 | done | `c81bd6d` |
| T4.1 | Прогресс: view, watch, expected, ETA, Observer | T3.1, T3.2, T3.3, T3.4 | todo | |
| T4.2 | `staged_pipeline.testing` | T4.1 | todo | |
| T5.1 | Интеграция tallyho | T4.1 | todo | |
| T5.2 | README и исполняемые примеры | T4.2 | todo | |
| T6.1 | CI: check.yml, release.yml, dependabot | T0.2 | todo | |
| T7.1 | GitHub через `.env`: `poe github` | T6.1 | todo | |
| T7.2 | PyPI pending trusted publisher | T7.1 | human | |
| T8.1 | Релиз 0.1.0a1 (тег — человек) | T5.1, T5.2, T7.1, T7.2 | todo | |
| T9.1 | Миграция checker_account (другой репозиторий) | T8.1 | todo | |

## Журнал

<!-- Новые записи сверху. Формат:
### 2026-10-24 · T0.1 · done
Что сделано. Отклонения от плана. Что узнали.
-->

### 2026-10-24 · T3.4 · done
`_limit.py`: `Limiter`/`Limit` (атомарная конкурентность через Condition + token bucket на Clock),
`weighted`; permit вокруг вызова/`__anext__`, emit вне. Гейт зелёный (148, 96.63%). Находка: захват
N permits семафором неатомарен → deadlock; сделал через счётчик+Condition.

### 2026-10-24 · T3.3 · done
`cancel`/`drain`/`run(timeout)`, `pipe.stopped`; супервизор различает намеренную отмену; offer в
закрытую стадию — `dropped_on_stop`. Гейт зелёный (144 теста, 96.69%).

### 2026-10-24 · T3.2 · done
Маршрутизация по типу (многоцелевые рёбра), промах→RoutingError/drop→unrouted; `executor='thread'`
(HandlerKind.SYNC, `asyncio.to_thread`), проверки sync/async↔executor. Гейт зелёный (138, 97.49%).

### 2026-10-24 · T3.1 · done
`SeenStore`/`SetSeenStore`, `offer()` (единый приём: dedup/resume/accept), событие `RESUME`
(found+skip), `Stage.resume()`, `seed_open()`→`Feeder`. Воркер переведён на карту стадий
(`stage.inbox`). Гейт зелёный (131 тест, 96.13%). Многоцелевая маршрутизация ещё без тестов (T3.2).

### 2026-10-24 · T2.4 · done
Воркер с классификацией: retry через defer (+ on_promote→REDELIVER), skip_on/error_on, прямые
Skip/Error, логирование исходов (log_levels), abort_if (после исхода и при закрытии). `log_levels` в
StageSpec/stage(). Гейт зелёный (123 теста, 95.93%). Находка: тесты на retry с FakeClock обязаны
дождаться регистрации спящего (`clock._sleepers`) перед `advance`, иначе гонка между computed-delay
(now=0) и регистрацией `clock.sleep` после advance уводит дедлайн.

### 2026-10-24 · T2.3 · done
`_supervisor.py`, `_stage.py` (StageContext, рантайм Stage, воркер, нормализация, emit/route),
`_pipeline.py` (Pipeline/stage/seed/run). Публичный API (`Pipeline`/`Stage`/`StageContext`) в
`__init__`. Гейт зелёный (114 тестов, 95.20%). Решения/находки: D-012 (точный `Stage[In,Out]`
отложен; `stage()` возвращает нераспараметренный `Stage`); `_supervisor` ниже `_stage` в слоях
(воркер принимает `Supervisor`); `run(timeout=)` принимается, но не используется до T3.3; многоцелевая
маршрутизация реализована минимально (тесты — в T3.2). basedpyright-all снова потребовал каст вызовов
хендлера (`Awaitable`/`AsyncGenerator`) и аннотаций всех атрибутов.

### 2026-10-24 · T2.2 · done
`_accounting.py`: `StageCounts`, `Ledger.transition` (единая точка смены счётчиков), компоненты из
SCC с pending/writers и каскадным закрытием. conftest с hypothesis-профилем. Гейт зелёный (104
теста, 98.61%); hypothesis-машина над 3 стадиями с циклом держит инвариант и согласованность
закрытия. `_accounting` над `_graph` в import-linter.

### 2026-10-24 · T2.1 · done
`_graph.py`: разбор хендлеров (`analyze_handler`), `StageSpec` + `.to()`, `build_route_map`,
`strongly_connected_components` (итеративный Тарьян), `validate_topology` (все проверки §7.6). Гейт
зелёный (93 теста, 98.35%). Решения/находки: D-011 (`_graph` выше `_policy`); убран mypy
`disallow_any_explicit` (ругал `Callable[..., object]` хендлера; литеральный `Any` ловит ruff);
ruff `ISC003` отключён (конфликтует с basedpyright `reportImplicitStringConcatenation` — используем
явный `+`); для тестов ослаблен `reportUnusedParameter` (фиктивные хендлеры). basedpyright «all»
требует: каст результатов `typing.get_args` (они `Any`), `collections.abc` вместо `typing.*`,
аннотации всех атрибутов.

### 2026-10-24 · T1.3 · done
`_inbox.py`: `Envelope[T]`, `Inbox[T]` (PEP 695 generics). get() ждёт на put/дозревании
отложенного по `Clock`/закрытии через `asyncio.wait({waiter, timer})`; cancel() отдаёт остаток
(ready FIFO + deferred по not_before). put/defer в закрытый → `StageClosedError`. Гейт зелёный
(73 теста, 98.92%).

### 2026-10-24 · T1.2 · done
`_policy.py`: `Expo`/`Fixed`/`Retry`, `classify()` (ближайший класс по MRO: Skip/Error → отложенный
ретрай → skip_on/error_on → Failure), `validate_policies()`, предохранители `ErrorRate`/`NoSuccess`
с `reason()`. Решения классификатора (`Deferral`/`SkipOutcome`/`ErrorOutcome`/`Failure`) — внутренние.
Гейт зелёный (58 тестов, 99.11%). Находки: `2 ** n` типизируется как `Any` (отрицательная степень) —
использую `2.0 ** n`; `_validate_label` переименован в `validate_label` (кросс-модульный приват
ругает basedpyright).

### 2026-10-24 · T1.1 · done
`_errors.py` (иерархия + `Outcome`/`Skip`/`Error` с валидацией метки) и `_clock.py` (`Clock`,
`MonotonicClock`, `FakeClock`). Публичный API в `__init__`. `api` (verifytypes) возвращён в гейт,
import-linter дополнен independence-контрактом фундамента. Гейт зелёный (26 тестов, покрытие 98.77%).
Мелочь: N818 для `StageAborted` подавлена точечно (имя из §4.5); убран `isinstance(label, str)` —
тип гарантирует `str` (basedpyright reportUnreachable); для тестов ослаблены reportPrivateUsage/
reportUnannotatedClassAttribute (белый ящик).

### 2026-10-24 · T0.2 · done
Гейты: группы lint/test/dev (версии закреплены, ставятся из кэша uv), строгие ruff/mypy/basedpyright,
import-linter, pytest+cov≥90%, poe-задачи, `.pre-commit-config.yaml`, тест-заглушка. Локальный
`uv run poe check` зелёный (ruff, mypy, basedpyright, import-linter, pytest 100%).

Отклонения и находки (оффлайн-среда, D-009/D-010):
- `codespell`, `deptry`, `check-jsonschema` вынесены из синхронизируемых групп (их нет в кэше, одна
  некэшированная зависимость роняет весь оффлайн-`uv sync`). deptry/codespell — через `uvx` (сеть/CI),
  check-github-workflows — через pre-commit на CI.
- Локальный venv на Python **3.14**: в кэше нет cp313-колёс компилируемых пакетов (mypy), а под 3.14
  всё есть. Код и тип-чекеры настроены на 3.13+ — семантика проверяется корректно. На CI
  (сеть есть) всё ставится и под 3.13.
- `api` (verifytypes) временно вне `poe check`: на скелете 0 публичных символов → 0% completeness
  (ложное срабатывание). Возвращается в T1.1.
- Слоевой контракт import-linter вводится инкрементально (D-010); на скелете — только forbidden
  «ядро не импортирует tallyho».
- Гейты запускаю как `uv run --offline --python 3.14 poe check`.

### 2026-10-24 · T0.1 · done
Скелет репозитория: пакет `staged_pipeline`, сборка hatchling, метаданные PyPI, LICENSE/README/
CHANGELOG и dot-файлы, AGENTS.md/CLAUDE.md. `uv sync`/`uv build` зелёные, импорт из колеса — версия
`0.1.0`. Первый коммит (документы) — на `main`, работа — в `impl/v1`.

Отклонения и находки:
- Репозиторий лежит внутри родительского uv-workspace (`~/pyproject.toml` с
  `[tool.uv.workspace]`, где `stage-pipeline` — member). Это ставило venv в домашнюю папку и тянуло
  зависимости соседей. Добавил в `pyproject.toml` пустой `[tool.uv.workspace]` — проект стал
  самостоятельным корнем.
- Среда **оффлайн** (DNS не резолвит, PyPI недоступен). Всё ставится из кэша uv через `--offline`.
  Следствие для тулчейна — [D-009](DECISIONS.md).
- `.idea/` случайно попал в первый коммит документов; добавлен в `.gitignore` и убран из отслеживания
  в коммите T0.1.
