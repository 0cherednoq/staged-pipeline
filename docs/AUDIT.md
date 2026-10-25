# Аудит: уязвимые места и прогресс исправлений

> Критический разбор библиотеки (2026-10-25). Статусы: `open` · `in_progress` · `fixed` ·
> `wontfix` · `deferred`. Чинится сверху вниз по критичности. Каждое исправление — с тестом
> и ссылкой на коммит.

## Сводка

| ID | Критичность | Место | Статус | Коммит |
|---|---|---|---|---|
| V2 | critical | `drain()` + bounded `put` → ложный сбой вместо остановки | fixed | — |
| V4 | high | исключение в `Observer` роняет пайплайн | fixed | — |
| V1 | high | `ctx.incr`/`ctx.label` из sync-хендлера (`executor="thread"`) — гонка | open | |
| V3 | medium | `executor="thread"`: `workers=` упирается в дефолтный пул; `to_thread` не отменяется | open | |
| V5 | medium | unbounded по умолчанию → OOM; `warn_queued` только предупреждает | open | |
| V6 | medium | `SeenStore` растёт вечно и недоступен через `Pipeline.stage()` | open | |
| V7 | low | исключение в `key=` атрибутируется не той стадии / при seed не обёрнуто | open | |
| V8 | low | `run(timeout=)` меряет wall-clock, а не `Clock` | open | |
| V9 | low | рекурсивный `_check_close` → `RecursionError` на глубоких графах | open | |
| V10 | low | `cancel()`/`drain()` до `run()` игнорируются | open | |
| V11 | cosmetic | после `cancel()` in-flight навсегда остаётся в счётчике | open | |
| V12 | low | неоднозначная маршрутизация по подтипам всплывает только в рантайме | open | |

## Детали

### V2 · critical · `drain()` + bounded-буфер = ложное падение

**Симптом.** Воркер висит на `await inbox.put(...)` в полный bounded-downstream; приходит
`drain()`. `drain` → `_cancel_queues` → `inbox.cancel()` будит `put` с `StageClosedError`.
`_handle` ловит `Exception` → классифицирует → `Failure`; `drain` (в отличие от `cancel`) не
ставит `_stopping` в супервизоре → `run()` бросает `StageClosedError` вместо `stopped="drain"`.

**Триггер.** Любой пайплайн с `buffer=<int>` у downstream и `drain()` при заполненной очереди.

**Исправление.** `offer` оборачивает постановку в очередь: `StageClosedError` на `put` =
«получатель закрыт» → элемент учитывается как `cancelled` (баланс `ACCEPT`), не как сбой.
Семантически emit в закрытую стадию всегда дроп, а в нормальной работе downstream не может
закрыться, пока upstream пишет, — так что маскировки реальных багов нет.

### V4 · high · исключение в `Observer` роняет пайплайн

**Симптом.** `observer.item_finished`/`stage_closed`/`breaker_tripped`/`queued_warning` зовутся
без защиты. Кривой пользовательский observer или падение логгера = сбой всего прогона.

**Исправление.** Пайплайн оборачивает наблюдателя в `_SafeObserver`: каждый вызов в try/except,
исключение логируется и проглатывается. Наблюдение не влияет на исход прогона.

## Риски первого запуска (не баги кода, снимаются на CI)

Эти пункты — следствие оффлайн-разработки (D-009); закроются первым зелёным прогоном CI:

- весь код гонялся только на Python 3.14; 3.13 (нижняя граница, D-002) — впервые на CI;
- `pre-commit`, `codespell`, `deptry` ни разу не исполнялись локально;
- workflow-файлы не валидированы против схемы GitHub Actions (версии action'ов, напр.
  `astral-sh/setup-uv`, не проверены);
- `poe bench` ссылается на несуществующий `scripts/bench.py`.
