from __future__ import annotations

from django.urls import path

from stratbot.alerts.views import UserSetupAlertListView

app_name = "alerts"
urlpatterns = [
    path(
        "setups/", view=UserSetupAlertListView.as_view(), name="user-setup-alert-list"
    ),
]
