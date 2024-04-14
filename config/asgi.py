import os
from django.core.asgi import get_asgi_application
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings.prod")

from channels.auth import AuthMiddlewareStack
from channels.routing import ProtocolTypeRouter, URLRouter
from django.urls import path

# from stratbot.scanner import consumers


# websocket_urlpatterns = [
#     path('ws/symbols/', consumers.SymbolRecConsumer.as_asgi()),
# ]

application = ProtocolTypeRouter(
    {
        "http": get_asgi_application(),
        # "websocket": AuthMiddlewareStack(
        #     URLRouter(
        #         websocket_urlpatterns,
        #     )
        # ),
    }
)