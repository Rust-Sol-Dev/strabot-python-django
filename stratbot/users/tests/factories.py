from __future__ import annotations

from typing import Any, Sequence

import factory
from factory import Faker
from factory.django import DjangoModelFactory

from stratbot.users.models import User


class UserFactory(DjangoModelFactory):
    email = factory.Sequence(lambda n: f"email{n}@tests.app.stratalerts.com")
    name = Faker("name")

    @factory.post_generation
    def password(self, create: bool, extracted: Sequence[Any], **kwargs):
        password = (
            extracted
            if extracted
            else Faker(
                "password",
                length=42,
                special_chars=True,
                digits=True,
                upper_case=True,
                lower_case=True,
            ).evaluate(None, None, extra={"locale": None})
        )
        self.set_password(password)

    class Meta:
        model = User
