from __future__ import annotations

from django.conf import settings
from twelvedata import TDClient


client = TDClient(settings.TWELVEDATA_API_KEY)
