from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from pusher import Pusher

from .clients import client

if TYPE_CHECKING:
    from stratbot.alerts.models import UserSetupAlert

logger = logging.getLogger(__name__)


class PusherChannelsBridge:
    def __init__(self, client: Pusher):
        self.client = client

    def trigger_one_channel(
        self, channel_name: str, event_name: str, data: dict[str, Any]
    ) -> None:
        client.trigger(channel_name, event_name, data)

    def deliver_user_setup_alert(self, alert: UserSetupAlert) -> None:
        user = alert.user
        channel_name = user.realtime_alerts_channel_name
        event_name = alert.realtime_event_name
        data = (alert.data or {}) | {"id": alert.id}
        return self.trigger_one_channel(channel_name, event_name, data)


channels_bridge = PusherChannelsBridge(client)
