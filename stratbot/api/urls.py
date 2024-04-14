from django.urls import path, include
from .views import (
    SetupListApiView,
)

urlpatterns = [
    path('setups/<str:symbol>', SetupListApiView.as_view()),
]
