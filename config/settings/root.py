from __future__ import annotations

from pathlib import Path

import environ

ROOT_DIR = Path(__file__).resolve(strict=True).parent.parent.parent
# stratbot/
APPS_DIR = ROOT_DIR / "stratbot"

# NOTE: We are using `interpolate=True` which means that environment variables starting
# with $ will be interpolated. See
# 1. https://django-environ.readthedocs.io/en/latest/tips.html#proxy-value and
# 2. https://django-environ.readthedocs.io/en/latest/tips.html#escape-proxy for more
# information.
# ! Important NOTE for this: We can rely on this for our own environment variables that
# _aren't_ used by third-party packages, but shouldn't rely on it otherwise (because
# third-party packages won't necessarily load in environment variables with some sort of
# `interpolate=True` type environment load, etc.).
env = environ.Env(interpolate=True)

READ_DOT_ENV_FILE = env.bool("DJANGO_READ_DOT_ENV_FILE", default=True)
if READ_DOT_ENV_FILE:
    # OS environment variables take precedence over variables from .env
    env.read_env(str(ROOT_DIR / ".env"))
