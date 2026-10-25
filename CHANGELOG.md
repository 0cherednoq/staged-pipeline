# Changelog

Все заметные изменения этого проекта документируются здесь.

Формат основан на [Keep a Changelog](https://keepachangelog.com/ru/1.1.0/),
проект придерживается [семантического версионирования](https://semver.org/lang/ru/).

## [Unreleased]

## [0.1.0a1] - 2026-10-25

Первый альфа-релиз. Полный API v0.1 (architecture.md §4).

### Added

- Ядро конвейера: `Pipeline`/`Stage`, пул воркеров на стадию, супервизор (fail-fast,
  плоское исключение), вывод режима завершения из графа (DAG по писателям, цикл по quiescence).
- Политики исходов: `retry=` с backoff (`Expo`/`Fixed`), `skip_on`/`error_on`, `Skip`/`Error`,
  предохранители `ErrorRate`/`NoSuccess`.
- Дедупликация (`key=`, `SeenStore`), `seed`/`resume`/`seed_open` (`Feeder`).
- Маршрутизация по типу выхода, `executor="thread"`, лимиты (`Limit`: concurrency + rate).
- Остановка: `cancel`/`drain`/таймаут.
- Прогресс: `view`/`watch`/`PipelineReport`, EWMA rate/latency, `expected`/`eta`, `Observer`.
- `staged_pipeline.testing` (`run_stage`, `FakeClock`), интеграция `integrations.tallyho`.
- CI (check/release), документация (README, docs/guide.md, docs/architecture.md).

### Known limitations (alpha)

- Открытые пункты аудита (docs/AUDIT.md): V1 (гонка `ctx.incr` из sync-хендлера), V3, V5–V12.
- API может меняться до 1.0.

[Unreleased]: https://github.com/0cherednoq/staged-pipeline/compare/v0.1.0a1...HEAD
[0.1.0a1]: https://github.com/0cherednoq/staged-pipeline/releases/tag/v0.1.0a1
