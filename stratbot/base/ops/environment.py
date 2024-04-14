from __future__ import annotations

from django.conf import settings

from stratbot.base.types.environment import Environment


def get_environment() -> Environment:
    environment = settings.ENVIRONMENT
    if isinstance(environment, Environment):
        return environment
    return Environment(environment)  # type: ignore
