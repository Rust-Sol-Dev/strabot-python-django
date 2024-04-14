from __future__ import annotations

from django.conf import settings
from polygon import RESTClient


client = RESTClient(settings.POLYGON_API_KEY)
