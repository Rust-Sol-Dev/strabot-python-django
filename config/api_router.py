from __future__ import annotations

from django.conf import settings
from django.urls import path
from rest_framework.routers import DefaultRouter, SimpleRouter

from stratbot.scanner.api.views import SymbolRecViewSet, SetupViewSet, MarketCalendarView


app_name = "api"

router: SimpleRouter
if settings.DEBUG:
    router = DefaultRouter()
else:
    router = SimpleRouter()

# router.register("users", UserViewSet)
router.register("setups", SetupViewSet)
router.register("symbols", SymbolRecViewSet)

urlpatterns = router.urls + [
    path('calendar/', MarketCalendarView.as_view(), name='calendar'),
]
