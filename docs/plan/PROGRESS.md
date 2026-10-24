# staged-pipeline v0.1 — прогресс

> Протокол обновления — [PLAN.md §0](PLAN.md#0-протокол-итерации). Файл правится в каждой итерации и
> коммитится вместе с задачей.
> Статусы: `todo` · `in_progress` · `done` · `blocked` (причина в журнале) · `human` (нужен человек).

## Текущее состояние

* **Ветка:** `impl/v1`
* **Текущая задача:** T0.2 (in_progress)
* **Последний зелёный коммит:** `3945150` (T0.1)
* **Среда:** оффлайн (нет DNS/PyPI), тулчейн ставится из кэша uv — см. [D-009](DECISIONS.md).

## Задачи

| ID | Задача | Зависит | Статус | Коммиты |
|---|---|---|---|---|
| T0.1 | Скелет репозитория | — | done | `3945150` |
| T0.2 | Гейты: линтеры, типы, import-linter, pytest, pre-commit | T0.1 | in_progress | |
| T1.1 | Ошибки, исходы `Skip/Error`, `Clock`/`FakeClock` | T0.2 | todo | |
| T1.2 | Политики: `Retry`, классификация, `ErrorRate`/`NoSuccess` | T1.1 | todo | |
| T1.3 | `Inbox`: готовые + отложенные, close/cancel с остатком | T1.1 | todo | |
| T2.1 | Граф: стадии, рёбра, типы из аннотаций, SCC, проверки | T1.1 | todo | |
| T2.2 | Учёт: функция перехода, счётчики, закрытие компонент | T2.1, T1.3 | todo | |
| T2.3 | Воркер, супервизор, `Pipeline.run` | T2.2, T1.2 | todo | |
| T2.4 | Политики в воркере, логирование, `abort_if` | T2.3 | todo | |
| T3.1 | Dedup, `seed`, `resume`, `seed_open` | T2.4 | todo | |
| T3.2 | Маршрутизация по типу, `executor="thread"` | T2.4 | todo | |
| T3.3 | `cancel`, `drain`, таймаут | T2.4 | todo | |
| T3.4 | Лимиты | T2.4 | todo | |
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
