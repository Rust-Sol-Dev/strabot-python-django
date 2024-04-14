from __future__ import annotations

from typing import Any

from allauth.account.adapter import DefaultAccountAdapter
from allauth.socialaccount.adapter import DefaultSocialAccountAdapter
from allauth.account.models import EmailAddress
from django.conf import settings
from django.http import HttpRequest


class AccountAdapter(DefaultAccountAdapter):
    def is_open_for_signup(self, request: HttpRequest):
        return getattr(settings, "ACCOUNT_ALLOW_REGISTRATION", True)


class SocialAccountAdapter(DefaultSocialAccountAdapter):
    def pre_social_login(self, request, sociallogin):
        # Ignore existing social accounts, just to be safe
        if sociallogin.is_existing:
            return

        # Check if the email is already in use
        try:
            email_address = EmailAddress.objects.get(email__iexact=sociallogin.user.email)
        except EmailAddress.DoesNotExist:
            # If the email is not in use, let allauth handle the signup
            return

        # If the email is already in use, connect this new social login to the existing user
        sociallogin.connect(request, email_address.user)

    def is_open_for_signup(self, request: HttpRequest, sociallogin: Any):
        return getattr(settings, "ACCOUNT_ALLOW_REGISTRATION", True)

    def is_auto_signup_allowed(self, request, sociallogin):
        # If email is already verified, connect the account automatically
        if sociallogin.account.user.email_verified:
            return True
        # Otherwise, use the default behavior
        return super().is_auto_signup_allowed(request, sociallogin)
