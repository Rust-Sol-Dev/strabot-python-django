from __future__ import annotations

from typing import Any

from django.conf import settings
from django.http import HttpRequest

from .models import UserSetupAlert


def pusher(request: HttpRequest) -> dict[str, Any]:
    data = {
        "pusher_key": settings.PUSHER_KEY,
        "pusher_cluster": settings.PUSHER_CLUSTER,
        "user_setup_alert_realtime_event_name": UserSetupAlert.realtime_event_name,
    }
    if request.user.is_authenticated:
        data[
            "user_setup_alert_realtime_alerts_channel_name"
        ] = request.user.realtime_alerts_channel_name
    else:
        data["user_setup_alert_realtime_alerts_channel_name"] = ""
    return data
