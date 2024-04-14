from __future__ import annotations

from django.conf import settings
from tda.auth import client_from_token_file

from config.integrations.tda import ops as tda_config_ops


tda_config_ops.sync_credentials_file(
    settings.TDA_CREDENTIALS_PATH,
    settings.TDA_EXISTING_CREDENTIALS_JSON_STR,
)

client = client_from_token_file(
    settings.TDA_CREDENTIALS_PATH,
    settings.TDA_CLIENT_ID,
    asyncio=False,
)
