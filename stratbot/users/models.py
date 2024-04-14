from __future__ import annotations

import secrets
from typing import Any

from django.contrib import auth
from django.contrib.auth.hashers import make_password
from django.contrib.auth.models import (
    AbstractBaseUser,
    BaseUserManager,
    Permission,
    PermissionsMixin,
)
from django.core.mail import send_mail
from django.db import models
from django.urls import reverse
from django.utils import timezone
from django.utils.translation import gettext_lazy as _


class UserManager(BaseUserManager):
    """
    Most all of this `UserManager` class was copied directly from Django 3.2.13 on
    2022-05-10. It was then modified to work without `username` (using only `email`
    instead of `username` + `email`, etc.).

    Django 4.0 !INSPECT_WHEN_UPGRADING_PAST_DJANGO_32: Do we want to sync with latest
    Django 4.0+ changes and update the note above?
    """

    use_in_migrations = True

    def _create_user(self, email: str, password: str, **extra_fields: Any) -> User:
        """
        Create and save a user with the given email and password.
        """
        if not email:
            raise ValueError("`email` missing")
        if not password:
            raise ValueError("`password` missing")
        email = self.normalize_email(email)
        user = self.model(email=email, **extra_fields)
        user.password = make_password(password)
        user.save(using=self._db)
        return user

    def create_user(
        self,
        email: str,
        password: str,
        **extra_fields,
    ):
        extra_fields.setdefault("is_staff", False)
        extra_fields.setdefault("is_superuser", False)
        return self._create_user(email, password, **extra_fields)

    def create_superuser(
        self,
        email: str,
        password: str,
        **extra_fields,
    ):
        extra_fields.setdefault("is_staff", True)
        extra_fields.setdefault("is_superuser", True)

        if extra_fields.get("is_staff") is not True:
            raise ValueError("Superuser must have `is_staff=True`.")
        if extra_fields.get("is_superuser") is not True:
            raise ValueError("Superuser must have `is_superuser=True`.")

        return self._create_user(email, password, **extra_fields)

    def with_perm(
        self,
        perm: Permission | None,
        is_active: bool = True,
        include_superusers: bool = True,
        backend: str | None = None,
        obj: User | None = None,
    ):  # pragma: no cover
        if backend is None:
            backends = auth._get_backends(return_tuples=True)  # type: ignore
            if len(backends) == 1:
                backend, _ = backends[0]
            else:
                raise ValueError(
                    "You have multiple authentication backends configured and "
                    "therefore must provide the `backend` argument."
                )
        elif not isinstance(backend, str):
            raise TypeError(
                "backend must be a dotted import path string (got %r)." % backend
            )
        else:
            backend = auth.load_backend(backend)  # type: ignore
        if hasattr(backend, "with_perm"):
            return backend.with_perm(  # type: ignore
                perm,
                is_active=is_active,
                include_superusers=include_superusers,
                obj=obj,
            )
        return self.none()


class User(AbstractBaseUser, PermissionsMixin):
    """
    An abstract base class implementing a fully featured User model with
    admin-compliant permissions.

    Email is required. Other fields are optional.
    """

    email = models.EmailField(_("Email"), unique=True)
    name = models.CharField(_("Name"), max_length=255)

    profile_image = models.ImageField(
        _("Profile Image"),
        upload_to="users/profile_images",
        null=True,
        blank=True,
        help_text=_("Optional."),
    )
    is_staff = models.BooleanField(
        _("Is Staff?"),
        default=False,
        help_text=_("Designates whether the user can log into this admin site."),
    )
    is_active = models.BooleanField(
        _("Is Active?"),
        default=True,
        help_text=_(
            "Designates whether this user should be treated as active. "
            "Potentially unselect this instead of deleting accounts."
        ),
    )
    date_joined = models.DateTimeField(_("Date Joined"), default=timezone.now)

    # NOTE: At the time of writing, we're using Pusher to deliver these alerts.
    # https://pusher.com/docs/channels/server_api/http-api/#examples-publish-an-event-on-a-single-channel
    # has an example of including the `channel_name`. This is a channel that's
    # _specific_ to this `User` so that the alerts are customized to this `User` and
    # only what this `User` wants/wanted.
    realtime_alerts_channel_name = models.CharField(
        "Realtime Alerts Channel Name",
        max_length=40,
        unique=True,
        editable=False,
        help_text=(
            "This is a unique realtime alerts channel name for this `User`. "
            "We make this value to be at least relatively secure and not guessable so "
            "that, at the time of writing, only Pusher knows the value. Browser "
            "extensions, etc. and other Javascript scripts can see it, but we're not "
            "worried about that because you have to be authenticated with Pusher to "
            "send messages to the channel anyway. So, we could've just used the `pk`, "
            "but for ever so slightly more peace of mind and potential security "
            "improvements we generate a long random string instead just using some "
            "value derived from the `pk`."
        ),
    )

    objects = UserManager()

    EMAIL_FIELD = "email"
    USERNAME_FIELD = "email"
    REQUIRED_FIELDS = ["name"]

    class Meta:
        verbose_name = _("User")
        verbose_name_plural = _("Users")

    def clean(self) -> None:
        super().clean()
        self.email = self.__class__.objects.normalize_email(self.email)

    def save(self, *args, **kwargs) -> None:
        is_insert = self.pk is None or self._state.adding

        if is_insert or not self.realtime_alerts_channel_name:
            self.realtime_alerts_channel_name = (
                self._generate_realtime_alerts_channel_name()
            )

        super().save(*args, **kwargs)

    def get_full_name(self) -> str:
        return (self.name or "").strip()

    def get_short_name(self) -> str:
        name_stripped = (self.name or "").strip()
        name_split = name_stripped.split()
        return name_split[0] if name_split else ""

    def email_user(
        self, subject: str, message: str, from_email: str | None = None, **kwargs: Any
    ) -> int:
        return send_mail(subject, message, from_email, [self.email], **kwargs)

    def get_absolute_url(self) -> str:
        """Get url for user's detail view.

        Returns:
            str: URL for user detail.

        """
        return reverse("users:detail", kwargs={"pk": self.pk})

    @staticmethod
    def _generate_realtime_alerts_channel_name(
        *, prefix: str = "alerts--user--"
    ) -> str:
        if not prefix:
            raise ValueError("Must have a `prefix` provided at the time of writing.")
        if len(prefix) >= 16:
            raise ValueError(
                "Prefix (`prefix`) should not exceed `16` characters in length at the "
                "time of writing."
            )
        key_length = 24
        key = secrets.token_urlsafe(key_length * 4)[:key_length].replace("-", "_")
        channel_name = f"{prefix}{key}"
        assert 25 <= len(channel_name) <= 40, "Pre-condition"
        return channel_name
