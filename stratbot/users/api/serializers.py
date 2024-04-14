from __future__ import annotations

from rest_framework import serializers

from stratbot.users.models import User


class UserSerializer(serializers.ModelSerializer):
    class Meta:
        model = User
        fields = ["id", "email", "name", "url"]

        extra_kwargs = {"url": {"view_name": "api:user-detail", "lookup_field": "pk"}}
