from django.conf import settings

from stratbot.scanner.integrations.polygon.bridges import PolygonWebsocketBridge


url = 'wss://business.polygon.io/stocks'
base_subscriptions = {
    'FMV.{}',
    'T.{}',
}
polygon_ws = PolygonWebsocketBridge(url, settings.POLYGON_API_KEY, base_subscriptions)
