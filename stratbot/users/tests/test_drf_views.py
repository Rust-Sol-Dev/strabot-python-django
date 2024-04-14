from __future__ import annotations

import pytest
from django.test import RequestFactory

from stratbot.users.api.views import UserViewSet
from stratbot.users.models import User

pytestmark = pytest.mark.django_db


class TestUserViewSet:
    def test_get_queryset(self, user: User, rf: RequestFactory):
        view = UserViewSet()
        request = rf.get("/fake-url/")
        request.user = user

        view.request = request  # type: ignore

        assert user in view.get_queryset()

    def test_me(self, user: User, rf: RequestFactory):
        view = UserViewSet()
        request = rf.get("/fake-url/")
        request.user = user

        view.request = request  # type: ignore

        response = view.me(request)  # type: ignore

        assert response.data == {
            "id": user.id,
            "email": user.email,
            "name": user.name,
            "url": f"http://testserver/api/users/{user.pk}/",
        }
