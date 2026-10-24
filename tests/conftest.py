"""Общие настройки тестов."""

from hypothesis import HealthCheck, settings

# Профиль по правилам PLAN.md §0.2: быстрый набор, без per-example deadline.
settings.register_profile(
    "default",
    max_examples=100,
    deadline=None,
    suppress_health_check=[HealthCheck.too_slow],
)
settings.load_profile("default")
