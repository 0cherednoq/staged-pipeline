# Решения

> Формат: `D-NNN · статус · дата` — решение, почему, альтернативы. Статусы: `ACCEPTED`,
> `ACCEPTED (автономно, пересмотреть)`, `SUPERSEDED by D-NNN`.

### D-001 · ACCEPTED · 2026-10-24
Неклассифицированное исключение хендлера роняет пайплайн (fail-fast по умолчанию).
Почему: тихое проглатывание прячет системные сбои (мёртвая сессия, прокси); «терпимая» стадия
объявляет `error_on={Exception: "..."}` явно. Альтернатива — `error` с логом (Broadway) — отклонена.

### D-002 · ACCEPTED · 2026-10-24
Python `>=3.13`. Очередь стадии своя (`Inbox`), `asyncio.Queue.shutdown` не используется;
3.13 выбрана владельцем как нижняя граница стека (checker_account на 3.14).

### D-003 · ACCEPTED · 2026-10-24
Буфер стадии по умолчанию unbounded, `warn_queued` + метрика `queued`. Bounded внутри цикла —
`TopologyError`. Почему: между стадиями ходят ссылки; bounded — источник deadlock-ов
(research.md, правила 12–13); замер checker_account — общий потолок резал резолв 40 → 1.2/с.

### D-004 · ACCEPTED · 2026-10-24
Режим завершения выводится из графа (SCC), параметра `termination=` нет (architecture.md §7.2).

### D-005 · ACCEPTED · 2026-10-24
Выход хендлера — `return`/`yield`; исход ≠ ok — исключениями (`Skip`, `Error`, доменные через
`skip_on/error_on`). Словарь прогресса совпадает с `tallyho.Progress`; зависимости от tallyho нет.

### D-006 · ACCEPTED · 2026-10-24
Публикация на PyPI — Trusted Publishing (OIDC) из `release.yml`, как в tallyho и browser_pool:
токена PyPI нет ни в `.env`, ни в секретах. `.env` хранит только `GH_TOKEN` для
`scripts/github_bootstrap.py` (создание репозитория, environment `pypi`, защита `main`).

### D-007 · ACCEPTED · 2026-10-24
Тесты только быстрые: набор < 30 с, тест < 5 с, `FakeClock` вместо реального времени, без Docker,
нагрузочных и хаос-тестов. Покрытие ≥ 90%.

### D-008 · ACCEPTED (автономно, пересмотреть) · 2026-10-24
Имя пакета `staged-pipeline` (импорт `staged_pipeline`), на PyPI свободно на 2026-10-24.
Сменить можно до T7.1 без затрат.

### D-009 · ACCEPTED (автономно, пересмотреть) · 2026-10-24
Среда разработки оффлайн: DNS не резолвит, PyPI недоступен; всё ставится из кэша uv (`--offline`).
`codespell` и `check-jsonschema` в кэше отсутствуют, поэтому:
- в синхронизируемые dependency-groups их **не** кладём (одна некэшированная зависимость роняет весь
  оффлайн-`uv sync`), иначе тулчейн вообще не ставится;
- `poe spell` вызывает codespell через `uvx codespell` (эфемерно): локально оффлайн шаг падает на
  доступе к сети, на CI и при живой сети — работает;
- хуки `codespell` и `check-github-workflows` остаются в `.pre-commit-config.yaml` (pre-commit сам
  тянет окружения из PyPI — на CI это зелёно);
- локальный исполнимый гейт — `poe check` (без codespell/jsonschema, т.к. их нечем поставить);
  полный гейт с codespell/check-github-workflows проверяется на CI (T6.1/T7.1).
Почему: гейты (ruff, mypy, basedpyright, import-linter, deptry, pytest+cov) не ослаблены и
исполняются локально; недоступные строго из-за отсутствия сети инструменты вынесены на CI, где сеть
есть. Пересмотреть, когда появится сеть (вернуть codespell в группу и в `poe check`).
