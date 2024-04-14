from __future__ import annotations

from config import celery_app
from stratbot.users.models import User


@celery_app.task()
def get_users_count():
    """A pointless Celery task to demonstrate usage."""
    return User.objects.count()
