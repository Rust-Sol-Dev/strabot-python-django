from channels.routing import ProtocolTypeRouter, URLRouter
from django.conf.urls import url

from . import consumers

websocket_urlpatterns = [
    url(r'^ws/quotes/$', consumers.YourConsumer.as_asgi()),
]

application = ProtocolTypeRouter({
    'websocket': URLRouter(
        websocket_urlpatterns
    ),
})
