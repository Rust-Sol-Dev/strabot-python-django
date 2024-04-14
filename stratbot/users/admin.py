from __future__ import annotations

from django.contrib import admin
from django.contrib.auth import admin as auth_admin
from django.utils.translation import gettext_lazy as _

from stratbot.users.forms import UserAdminChangeForm, UserAdminCreationForm
from stratbot.users.models import User


@admin.register(User)
class UserAdmin(auth_admin.UserAdmin):
    form = UserAdminChangeForm
    add_form = UserAdminCreationForm
    fieldsets = (
        (None, {"fields": ("email", "password")}),
        (_("Personal info"), {"fields": ("name",)}),
        (
            _("Permissions"),
            {
                "fields": (
                    "is_active",
                    "is_staff",
                    "is_superuser",
                    "groups",
                    "user_permissions",
                ),
            },
        ),
        (_("Important dates"), {"fields": ("last_login", "date_joined")}),
    )
    add_fieldsets = (
        (
            None,
            {
                "classes": ("wide",),
                "fields": ("email", "name", "password1", "password2"),
            },
        ),
    )
    list_display = [
        "email",
        "name",
        "date_joined",
        "last_login",
        "is_superuser",
        "is_staff",
        "is_active",
    ]
    list_filter = [
        "date_joined",
        "last_login",
        "is_superuser",
        "is_staff",
        "is_active",
    ]
    search_fields = ["email", "name"]
    ordering = ["-pk"]
