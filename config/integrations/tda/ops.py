from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


@dataclass
class CredentialsFileSyncResult:
    created: bool
    updated: bool
    changed: bool | None
    wrote_to_file: bool
    path: str


def sync_credentials_file(
    credentials_path: str | Path,
    existing_credentials_json_str: str,
    *,
    log: bool = True,
) -> CredentialsFileSyncResult:
    path = (
        Path(credentials_path)
        if isinstance(credentials_path, str)
        else credentials_path
    )
    existing: dict[str, Any] = json.loads(existing_credentials_json_str)
    # Handle the case where we provided an empty value ({}) (intentionally or
    # unintentionally). You can intentionally do this to prevent the file from being
    # overwritten when you don't want that to happen.
    if not existing:
        return CredentialsFileSyncResult(
            created=False,
            updated=False,
            changed=None,
            wrote_to_file=False,
            path=str(path),
        )

    # Handle the case where the file doesn't exist yet.
    if not path.exists():
        with open(path, "w") as initial_f:
            json.dump(existing, initial_f)
        return CredentialsFileSyncResult(
            created=True,
            updated=True,
            changed=True,
            wrote_to_file=True,
            path=str(path),
        )

    changed: bool = False
    updated: bool = False
    wrote_to_file: bool = False
    if existing:
        with open(path, "r+") as update_f:
            contents = update_f.read().strip() or "{}"
            current: dict[str, Any] = json.loads(contents)
            # Only write if it changed for performance reasons.
            if not current or current != existing:
                update_f.seek(0)
                update_f.truncate()
                json.dump(existing, update_f)
                changed = True
                updated = True
                wrote_to_file = True

    result = CredentialsFileSyncResult(
        created=False,
        updated=updated,
        changed=changed,
        wrote_to_file=wrote_to_file,
        path=str(path),
    )
    if log:
        logger.info("Synced TDA Credentials file (%s). Result: %s", path, repr(result))
    return result


def obtain_credentials(*, asyncio: bool = False) -> None:
    assert (
        asyncio is False
    ), "If we allow `True` make sure this works for `True` and then remove this assertion."
    from django.conf import settings
    from selenium import webdriver
    from tda.auth import client_from_login_flow

    with webdriver.Chrome() as driver:
        client_from_login_flow(
            driver,
            settings.TDA_CLIENT_ID,
            settings.TDA_REDIRECT_URI,
            settings.TDA_CREDENTIALS_PATH,
            asyncio=asyncio,
        )
