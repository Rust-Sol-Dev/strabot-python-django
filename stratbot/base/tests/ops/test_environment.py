from __future__ import annotations

from stratbot.base.ops.environment import get_environment
from stratbot.base.types.environment import Environment


def test_get_environment(settings):
    settings.ENVIRONMENT = "dev"
    environment1 = get_environment()
    settings.ENVIRONMENT = Environment("dev")
    environment2 = get_environment()
    settings.ENVIRONMENT = "prod"
    environment3 = get_environment()
    settings.ENVIRONMENT = Environment("prod")
    environment4 = get_environment()

    assert environment1 == Environment.DEV
    assert environment1 is Environment.DEV
    assert environment1.is_dev
    assert not environment1.is_prod
    assert environment2 == Environment.DEV
    assert environment2 is Environment.DEV
    assert environment2.is_dev
    assert not environment2.is_prod
    assert environment3 == Environment.PROD
    assert environment3 is Environment.PROD
    assert not environment3.is_dev
    assert environment3.is_prod
    assert environment4 == Environment.PROD
    assert environment4 is Environment.PROD
    assert not environment4.is_dev
    assert environment4.is_prod
