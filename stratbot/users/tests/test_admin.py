from __future__ import annotations

import pytest
from django.urls import reverse

from stratbot.users.models import User

pytestmark = pytest.mark.django_db


class TestUserAdmin:
    def test_changelist(self, admin_client):
        url = reverse("admin:users_user_changelist")
        response = admin_client.get(url)
        assert response.status_code == 200

    def test_search(self, admin_client):
        url = reverse("admin:users_user_changelist")
        response = admin_client.get(url, data={"q": "test"})
        assert response.status_code == 200

    def test_add(self, admin_client):
        url = reverse("admin:users_user_add")
        response = admin_client.get(url)
        assert response.status_code == 200

        response = admin_client.post(
            url,
            data={
                "email": "some-test-email1@tests.scanner.stratalerts.com",
                "name": "Some Name",
                "password1": "My_R@ndom-P@ssw0rd",
                "password2": "My_R@ndom-P@ssw0rd",
            },
            follow=True,
        )
        assert response.status_code == 200
        assert User.objects.filter(
            email="some-test-email1@tests.scanner.stratalerts.com"
        ).exists()

    def test_view_user(self, user, admin_client):
        user = User.objects.get(email=user.email)
        url = reverse("admin:users_user_change", kwargs={"object_id": user.pk})
        response = admin_client.get(url)
        assert response.status_code == 200
