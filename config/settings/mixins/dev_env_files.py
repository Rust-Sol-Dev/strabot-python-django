from __future__ import annotations

from ..root import ROOT_DIR, env

READ_LOCAL_ENV_FILE = env.bool("DJANGO_READ_ENV_LOCAL_FILE", default=True)
READ_DEV_ENV_FILES = env.bool("DJANGO_READ_DEV_ENV_FILES", default=True)
# Read this one first so that, if enabled, the values in it take precedence.
if READ_LOCAL_ENV_FILE:
    env.read_env(ROOT_DIR / ".local.env")
if READ_DEV_ENV_FILES:
    envs_dir = ROOT_DIR / ".envs" / ".dev"
    # These files are in order of precedence.
    env.read_env(envs_dir / ".specific.env")
    env.read_env(envs_dir / ".secrets.env")
    env.read_env(envs_dir / ".django.env")
    env.read_env(envs_dir / ".postgres.env")
    env.read_env(envs_dir / ".redpanda.env")
    env.read_env(envs_dir / ".redis.env")
    env.read_env(envs_dir / ".celery.env")
