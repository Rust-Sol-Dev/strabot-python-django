from __future__ import annotations

from django.contrib.auth.mixins import LoginRequiredMixin
from django.views.generic.list import ListView

from stratbot.alerts.models import UserSetupAlert


class UserSetupAlertListView(LoginRequiredMixin, ListView):
    model = UserSetupAlert
    ordering = ["-created", "-pk"]

    # NOTE: To see example pagination in action with Django's current default pagination
    # setup, see
    # https://docs.djangoproject.com/en/3.2/topics/pagination/#paginating-a-listview..
    #
    # ^ That being said, you might want to look into
    # https://github.com/photocrowd/django-cursor-pagination or something like that
    # if/once the number of records becomes high enough (like maybe >= 50,000 or >=
    # 100,000 or per-`User` or so). You also just may want to prune older alerts with
    # timeframes less than a day, etc. Up to you how you want to do, those are just some
    # examples.
    # ...
    # paginate_by = 100  # if pagination is desired
    # ...

    def get_queryset(self):
        return super().get_queryset().filter(user=self.request.user)[:50]
