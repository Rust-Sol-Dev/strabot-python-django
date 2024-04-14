"""
Taken from Django 4.0 on 2022-05-30. At the time of writing, we're on Django 3.2 until
Django Celery Beat and any other dependencies properly support Django 4.0+ and Python
3.10+. Hence, I just manually copy/pasted this from Django 4.0 for now. This can be
removed and replaced with the native Django version once we upgrade (see
https://docs.djangoproject.com/en/dev/ref/contrib/postgres/expressions/#arraysubquery-expressions).
"""
from __future__ import annotations

from django.contrib.postgres.fields import ArrayField
from django.db.models import Subquery
from django.utils.functional import cached_property


class ArraySubquery(Subquery):
    template = "ARRAY(%(subquery)s)"

    def __init__(self, queryset, **kwargs):
        super().__init__(queryset, **kwargs)

    @cached_property
    def output_field(self):
        return ArrayField(self.query.output_field)  # type: ignore
