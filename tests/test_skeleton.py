"""Заглушка, чтобы pytest и покрытие работали на скелете (T0.2)."""

import staged_pipeline


def test_version_exposed() -> None:
    assert isinstance(staged_pipeline.__version__, str)
    assert staged_pipeline.__version__
