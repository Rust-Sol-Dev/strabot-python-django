from __future__ import annotations

from typing import Literal

from django.db.models import TextChoices

EnvironmentType = Literal["dev", "test", "ci", "stage", "prod"]


class Environment(TextChoices):
    # NOTE: Intentionally not using `gettext_lazy` here or providing nice `label`s.
    # Keeping it the same because we want to the value to display the same everywhere.
    # Mainly using `TextChoices` (which wants a `label` as well although can provide it
    # by default) because `StrEnum` isn't available in the standard library of Python
    # until 3.11+ (and we're on Python 3.10 at the time of writing).
    DEV = "dev", "dev"
    TEST = "test", "test"
    CI = "ci", "ci"
    STAGE = "stage", "stage"
    PROD = "prod", "prod"

    @property
    def is_dev(self) -> bool:
        return self == Environment.DEV

    @property
    def is_test(self) -> bool:
        return self == Environment.TEST

    @property
    def is_ci(self) -> bool:
        return self == Environment.CI

    @property
    def is_stage(self) -> bool:
        return self == Environment.STAGE

    @property
    def is_prod(self) -> bool:
        return self == Environment.PROD

    @property
    def is_running_tests(self) -> bool:
        return self.is_test or self.is_ci
