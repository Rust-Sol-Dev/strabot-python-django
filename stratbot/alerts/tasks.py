from __future__ import annotations
from datetime import datetime, timedelta

import msgspec.json

from config import celery_app
from django.utils import timezone

from dataflows.alerts import DiscordMsgAlert
from dataflows.setups import SetupMsg
from .models import UserSetupAlert, DiscordAlert
from .ops import user_setup_alerts as user_setup_alert_ops
from ..scanner.models.symbols import Setup, SymbolRec
from stratbot.events.models import CalendarEvent
from ..scanner.ops.economic_calendar import ForexFactory


@celery_app.task()
def deliver_user_setup_alert_via_realtime(user_setup_alert_pk: int) -> None:
    # TODO: If we add foreign keys to the `Setup` model make sure they're included in
    # the `select_related` call here.
    alert = UserSetupAlert.objects.select_related("user", "symbol_rec", "setup").get(
        pk=user_setup_alert_pk
    )
    # NOTE: Could add some simple retry logic here if desired down the line. Make sure
    # to call `alert.refresh_from_db()` first though so that the updated value from
    # `alert.realtime_attempt_count = F("realtime_attempt_count") + 1` is at the latest
    # value.
    user_setup_alert_ops.deliver_user_setup_alert_via_realtime(alert)


@celery_app.task()
def deliver_user_setup_alert_via_web_push(user_setup_alert_pk: int) -> None:

    # TODO: If we add foreign keys to the `Setup` model make sure they're included in
    # a `select_related` call here if we are doing one, etc.
    raise NotImplementedError("This is not currently implemented.")


@celery_app.task
def send_discord_alert(symbolrec_pk: int, setup_pk: int) -> None:
    symbolrec = SymbolRec.objects.get(pk=symbolrec_pk)
    setup = Setup.objects.get(pk=setup_pk)
    discord_alert = DiscordAlert(symbolrec, setup)
    discord_alert.send_msg(channel=symbolrec.symbol_type)
    setup.discord_alerted = True
    setup.save()


@celery_app.task
def send_discord_alert_from_dataflow(symbol: str, setup: bytes, channel: str = None) -> None:
    setup_msg = SetupMsg(**msgspec.json.decode(setup))

    symbolrec = SymbolRec.objects.get(symbol=symbol)
    discord_alert = DiscordMsgAlert(symbolrec, setup_msg)
    if channel is None:
        channel = symbolrec.symbol_type
    discord_alert.send_msg(channel=channel)


@celery_app.task()
def check_calendar_events():
    events = (
        CalendarEvent.objects
        .filter(currency='USD')
        .filter(impact__in=['Medium', 'High'])
        .filter(timestamp__gte=timezone.now(), timestamp__lte=timezone.now() + timedelta(minutes=15))
        .filter(discord_notified=False)
    )
    for event in events:
        event.send_to_discord()
        event.discord_notified = True
        event.save()


@celery_app.task()
def refresh_calendar_events():
    ff = ForexFactory()
    ff.send_to_discord()
    ff.df_to_db()
