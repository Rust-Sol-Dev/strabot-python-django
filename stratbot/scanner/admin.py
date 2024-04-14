from __future__ import annotations

from django.contrib import admin

from .models.symbols import SymbolRec
from .models.live_loop import LiveLoopRun, LiveLoop


from django.contrib.auth.admin import UserAdmin as DefaultUserAdmin
from stratbot.users.models import User


# TODO: currently broken
# class UserProfileInline(admin.StackedInline):
#     model = User
#     can_delete = False
#
#
# class UserAdmin(DefaultUserAdmin):
#     inlines = (UserProfileInline,)
#     ordering = ('email',)
#     list_display = ('email', 'name', 'is_staff', 'is_active')
#
#
# # Re-register UserAdmin
# admin.site.unregister(User)
# admin.site.register(User, UserAdmin)

admin.site.register(SymbolRec)
admin.site.register(LiveLoop)
admin.site.register(LiveLoopRun)
