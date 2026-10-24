"""Тесты тест-утилит staged_pipeline.testing (T4.2)."""

from staged_pipeline.testing import FakeClock, run_stage


class NotFound(Exception):
    pass


async def test_run_stage_collects_fanout_outputs() -> None:
    async def double(n: int) -> list[int]:
        return [n, n * 10]

    run = await run_stage(double, [1, 2])
    assert sorted(cast_ints(run.outputs)) == [1, 2, 10, 20]
    assert run.view.ok == 2


async def test_run_stage_skip_outcome() -> None:
    async def handler(n: int) -> None:
        if n == 2:
            raise NotFound

    run = await run_stage(handler, [1, 2, 3], skip_on={NotFound: "deleted"})
    assert run.view.skip == 1
    assert run.view.labels["deleted"] == 1
    assert run.view.ok == 2


async def test_run_stage_terminal_handler_no_outputs() -> None:
    async def handler(n: int) -> None:  # noqa: ARG001
        return None

    run = await run_stage(handler, [1, 2], clock=FakeClock())
    assert run.outputs == []
    assert run.view.ok == 2


async def test_run_stage_with_key_dedups() -> None:
    async def handler(n: int) -> int:
        return n

    def ident(n: object) -> object:
        return n

    run = await run_stage(handler, [1, 1, 2], key=ident)
    assert run.view.duplicates == 1
    assert run.view.ok == 2


def cast_ints(values: list[object]) -> list[int]:
    return [v for v in values if isinstance(v, int)]
