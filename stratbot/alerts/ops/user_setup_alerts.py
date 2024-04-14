from __future__ import annotations

import logging
from functools import lru_cache
from typing import Any

from django.db import transaction
from django.db.models import F
from django.utils import timezone

from stratbot.scanner.models.symbols import Setup, SymbolRec
from stratbot.users.models import User

from ..integrations.pusher.bridges import channels_bridge
from ..models import UserSetupAlert, AlertType

logger = logging.getLogger(__name__)


def create_user_setup_alert(
    *,
    user: User,
    symbol_rec: SymbolRec,
    setup: Setup,
    alert_type: AlertType,
    data: dict[str, Any],
    persist: bool,
    realtime_should_send: bool = True,
    # NOTE: Set `web_push_should_send` to `True` if/once web push notifications code is
    # done and it's ready to go and you want those being delivered.
    web_push_should_send: bool = False,
) -> UserSetupAlert:
    alert = UserSetupAlert(
        user=user,
        symbol_rec=symbol_rec,
        setup=setup,
        alert_type=alert_type,
        data=data,
        realtime_should_send=realtime_should_send,
        web_push_should_send=web_push_should_send,
    )
    if persist:
        alert.save()
        _check_and_queue_user_setup_alert_tasks(alert)
    return alert


def bulk_create_user_setup_alerts(alerts: list[UserSetupAlert]) -> None:
    # This basically just checks that:
    # 1. You passed in a `list`.
    # 2. That `list` has at least one element.
    # 3. That first element (the rest should also be like this) is a `UserSetupAlert` in
    # the adding state, which means that Django expects to perform an insert on it.
    assert (
        isinstance(alerts, list)
        and alerts
        and isinstance(alerts[0], UserSetupAlert)
        and alerts[0]._state.adding
    ), "Pre-condition"

    # NOTE: Could potentially use some unique constraints and `ignore_conflicts=False`
    # as a way for preventing duplicate alerts while letting the valid ones go through
    # in a race condition scenario.
    UserSetupAlert.objects.bulk_create(alerts, batch_size=500, ignore_conflicts=False)

    for alert in alerts:
        # NOTE: If we get to the point where we're _actually_ bulk creating more than
        # ~50-100+ `UserSetupAlert`s on average, it'd be pretty straightforward to add
        # another layer of indirection here where we pass in all the `pk` values of each
        # `UserSetupAlert` as a `list` of `pk`s into a Celery task that runs this in the
        # background. That would eliminate the ~50-100+ individual Redis calls that
        # would get made here.
        _check_and_queue_user_setup_alert_tasks(alert)


def deliver_user_setup_alert_via_realtime(alert: UserSetupAlert) -> None:
    if not alert.realtime_should_send:
        logger.info(
            (
                "Did not deliver `UserSetupAlert` with pk %d in realtime because "
                "`realtime_should_send == False`."
            ),
            alert.pk,
        )
        return
    if alert.realtime_sent:
        logger.info(
            (
                "Did not deliver `UserSetupAlert` with pk %d in realtime because "
                "`realtime_sent == True`."
            ),
            alert.pk,
        )
        return

    # NOTE: We'll use `update_fields=update_fields` below in the `save` so that if we're
    # delivering multiple types of notifications concurrently we don't run into data
    # race issues when saving to the database.
    update_fields: list[str] = [
        "modified",
        "realtime_sent",
        "realtime_sent_at",
        "realtime_attempt_count",
    ]
    try:
        channels_bridge.deliver_user_setup_alert(alert)
    except Exception as e:
        now = timezone.now()
        logger.exception(
            "Failed to deliver `UserSetupAlert` with pk %d due to an exception.",
            alert.pk,
        )
        exception_str = str(e)
        alert.realtime_error = exception_str
        alert.realtime_sent = None
        update_fields.append("realtime_error")
    else:
        now = timezone.now()
        alert.realtime_sent = True

    alert.realtime_sent_at = now
    alert.realtime_attempt_count = F("realtime_attempt_count") + 1
    alert.save(update_fields=update_fields)


def deliver_user_setup_alert_via_web_push(alert: UserSetupAlert):
    raise NotImplementedError("This is not currently implemented.")


def _check_and_queue_user_setup_alert_tasks(alert: UserSetupAlert) -> None:
    assert alert.pk is not None, "Pre-condition"

    if alert.realtime_should_send:
        realtime_task = _get_user_setup_alert_realtime_task()
        transaction.on_commit(lambda: realtime_task.delay(alert.pk))

    if alert.web_push_should_send:
        web_push_task = _get_user_setup_alert_web_push_task()
        transaction.on_commit(lambda: web_push_task.delay(alert.pk))


@lru_cache(maxsize=1, typed=False)
def _get_user_setup_alert_realtime_task():
    from stratbot.alerts.tasks import deliver_user_setup_alert_via_realtime as task

    return task


@lru_cache(maxsize=1, typed=False)
def _get_user_setup_alert_web_push_task():
    from stratbot.alerts.tasks import deliver_user_setup_alert_via_web_push as task

    return task
