"""Иерархия исключений и исходы элемента (architecture.md §4.3, §4.4).

Все исключения библиотеки — подклассы `StagedPipelineError`, кроме исходов
`Skip`/`Error`: они наследуют `Outcome(Exception)` и архитектурно отдельны —
это не сбой библиотеки, а объявленный результат обработки элемента.
"""

MAX_LABEL_LENGTH = 200
"""Максимальная длина метки исхода (символов)."""


class StagedPipelineError(Exception):
    """Базовый класс всех исключений библиотеки (кроме исходов `Skip`/`Error`)."""


class ConfigurationError(StagedPipelineError):
    """Неверная конфигурация стадии или пайплайна (например, класс в двух `*_on`)."""


class TopologyError(StagedPipelineError):
    """Граф не проходит проверки при старте (`run()` ничего не запускает, §7.6)."""


class RoutingError(StagedPipelineError):
    """Выход хендлера не подошёл ни к одной стадии-получателю в рантайме (§4.2)."""


class StageAborted(StagedPipelineError):  # noqa: N818  # имя из architecture.md §4.5
    """Стадия остановлена предохранителем, а исходного исключения нет (§4.5)."""


class StageClosedError(StagedPipelineError):
    """Попытка записи в закрытую стадию/вход (§7.1, правило 17 ресёрча)."""


def _validate_label(label: str) -> str:
    """Проверить метку исхода: непустая строка не длиннее `MAX_LABEL_LENGTH`.

    Args:
        label: метка исхода (`ok`/`skip`/`error`), заданная кодом.

    Returns:
        Ту же метку без изменений, если она валидна.

    Raises:
        ConfigurationError: метка пустая или слишком длинная — это ошибка в
            коде, а не исход элемента.
    """
    if not label.strip():
        raise ConfigurationError("метка исхода не может быть пустой")
    if len(label) > MAX_LABEL_LENGTH:
        raise ConfigurationError(f"метка исхода длиннее {MAX_LABEL_LENGTH} символов: {len(label)}")
    return label


class Outcome(Exception):  # noqa: N818  # не -Error: это не сбой, а исход элемента (§4.3)
    """Базовый класс исходов элемента, выражаемых исключением.

    Отдельно от `StagedPipelineError` (architecture.md §4.3): хендлер поднимает
    `Skip`/`Error`, чтобы объявить результат, а не сообщить о сбое библиотеки.
    """

    def __init__(self, label: str) -> None:
        """Создать исход с меткой.

        Args:
            label: метка исхода (непустая, не длиннее `MAX_LABEL_LENGTH`).
        """
        self.label: str = _validate_label(label)
        super().__init__(self.label)


class Skip(Outcome):
    """Элемент пропущен осознанно: метка попадёт в `skip` и в `labels` стадии."""


class Error(Outcome):
    """Элемент завершился ошибкой домена: метка попадёт в `error` и в `labels`."""


__all__ = [
    "ConfigurationError",
    "Error",
    "Outcome",
    "RoutingError",
    "Skip",
    "StageAborted",
    "StageClosedError",
    "StagedPipelineError",
    "TopologyError",
]
