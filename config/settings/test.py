"""
With these settings, tests run faster.
"""

from __future__ import annotations

import os

os.environ.setdefault("ENVIRONMENT", "test")
os.environ[
    "TDA_CREDENTIALS_PATH"
] = "config/integrations/tda/tda_credentials_for_tests.json"

from .mixins.dev_env_files import *  # noqa  # isort:skip
from .ci import *  # noqa  # isort:skip
