from __future__ import annotations

import os

from celery import Celery
from celery.schedules import crontab

# set the default Django settings module for the 'celery' program.
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings.dev")

app = Celery("stratbot")

# Using a string here means the worker doesn't have to serialize
# the configuration object to child processes.
# - namespace='CELERY' means all celery-related configuration keys
#   should have a `CELERY_` prefix.
app.config_from_object("django.conf:settings", namespace="CELERY")

# Load task modules from all registered Django app configs.
app.autodiscover_tasks()

# app.conf.beat_schedule = {
#     "queue_historical_data_fetch": {
#         "task": "stratbot.scanner.tasks.queue_historical_data_fetch",
#         # Run every fifth minute (see https://crontab.guru/#*/5_*_*_*_*).
#         "schedule": crontab(
#             minute="*/5", hour="*", day_of_week="*", day_of_month="*", month_of_year="*"
#         ),
#     }
# }
