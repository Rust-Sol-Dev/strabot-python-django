from __future__ import annotations

import os

os.environ.setdefault("ENVIRONMENT", "stage")

from .prod import *  # noqa  # isort:skip
