from __future__ import annotations

import pytest
from django.core.management import call_command

from stratbot.users.models import User


@pytest.mark.django_db
def test_all_created():
    email = "admin@stratalerts.com"
    password = "stratbot_admin_pw_9*"
    name = "Admin Superuser"
    call_command("create_default_admins")
    user = User.objects.get(email=email)

    assert user.check_password(password)
    assert user.name == name
    assert user.is_active is True
    assert user.is_staff is True
    assert user.is_superuser is True


@pytest.mark.django_db
def test_all_not_created():
    email = "admin@stratalerts.com"
    password = "stratbot_admin_pw_9*"
    name = "Admin Superuser"
    call_command("create_default_admins")
    user = User.objects.get(email=email)
    user.set_password(f"{password}1")
    user.name = "Superuser Admin"
    user.save()
    call_command("create_default_admins")

    assert not user.check_password(password)
    assert not user.name == name
    assert user.is_active is True
    assert user.is_staff is True
    assert user.is_superuser is True
