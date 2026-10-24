# Правила разработки staged-pipeline

Полный процесс — [docs/plan/PLAN.md](docs/plan/PLAN.md). Ниже — то, что нельзя нарушать (§0.3).

## Железные правила

- **Не ослаблять** линтеры, типизацию, покрытие (≥ 90%). Подавление — точечное и с причиной:
  `# ruff: ignore[rule]  # причина`, `# pyright: ignore[rule]  # причина`.
- Запрещены (banned-api): `typing.Any`, `asyncio.get_event_loop`, `logging.basicConfig`,
  `unittest.mock.patch`.
- **Ядро без зависимостей:** `dependencies = []`. Extras не импортируют внешних пакетов
  (интеграция tallyho — через `Protocol`, architecture.md §9.3).
- Все исключения библиотеки — подклассы `StagedPipelineError`, кроме исходов `Skip`/`Error`
  (они — `Outcome(Exception)`, архитектурно отдельны). Все модули объявляют `__all__`.
- Комментарии и docstring — на русском, идентификаторы — на английском. Google-style docstring.
- Решения architecture.md не менять молча: сначала запись в
  [docs/plan/DECISIONS.md](docs/plan/DECISIONS.md), потом код.
- Файлы, которые пишет Python, — с `newline="\n"`.

## Время и тесты

- Время — только через `Clock`; в тестах `FakeClock`. Реальные `asyncio.sleep` ≤ 10 мс и только там,
  где проверяется сам event loop.
- Весь набор тестов < 30 с, `pytest-timeout = 5` на тест. Нет Docker, нагрузочных и хаос-тестов.

## Гейты (Definition of Done)

- `uv run poe check` — формат, линт, типы, import-linter, deptry, codespell, pytest + покрытие.
- `uv run pre-commit run --all-files`.
- DoD конкретной карточки задачи в PLAN.md.

## Коммиты

- Маленькие, одно логическое изменение. Сообщение на русском, заголовок `T<id>: <что>` ≤ 72 символов.
- Ветка работы — `impl/v1`; коммиты в `main` запрещены pre-commit-хуком.
