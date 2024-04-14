from __future__ import annotations

import time

import pyperclip
from django.core.management.base import BaseCommand, CommandError

from stratbot.base.ops.environment import get_environment
from stratbot.users.models import User


class Command(BaseCommand):
    """
    Create default admin `User`(s) and any other associated records.

    Not intended for production use. For production, use `python manage.py
    createsuperuser` and explicitly set the email + password + anything else there.
    """

    help = __doc__

    def handle(self, *args, **options):
        environment = get_environment()
        if not environment.is_dev and not environment.is_running_tests:
            raise CommandError(
                "Cannot run this command outside of the dev/test/ci environments right "
                "now."
            )

        email = "admin@stratalerts.com"
        password = "stratbot_admin_pw_9*"
        defaults = {
            "name": "Admin Superuser",
            "is_active": True,
            "is_superuser": True,
            "is_staff": True,
        }
        admin_user = User.objects.filter(email=email).first()
        admin_user_created = False
        if admin_user is None:
            admin_user = User.objects.create_superuser(email, password, **defaults)
            admin_user_created = True

        if admin_user_created:
            self.stdout.write(
                self.style.SUCCESS(
                    f"Admin superuser for {email} was created: {admin_user!r}. The "
                    f"password is `{password}` (inside the `s)."
                )
            )
        else:
            self.stdout.write(
                self.style.NOTICE(
                    f"Admin superuser for {email} already existed: {admin_user!r}. "
                    "The default password the superuser is initially created with "
                    f"is `{password}` (inside the `s)."
                )
            )

        try:
            pyperclip.copy(email)
            # See
            # https://github.com/asweigart/pyperclip/issues/196#issuecomment-912801976.
            # It seems like a slight pause is needed to lock lock up access to the
            # clipboard and have this work for, for example, Windows clipboard history
            # (Windows Key + v by default).
            time.sleep(0.250)
            pyperclip.copy(password)
        except Exception as e:
            self.stdout.write(
                self.style.NOTICE(
                    "Unable to copy email and password to the clipboard. You'll have "
                    f'to copy paste and/or input them manually. Exception: "{e}".'
                )
            )
        else:
            self.stdout.write(
                "Copied email (first copy) and password (second copy) to the system "
                "clipboard."
            )
