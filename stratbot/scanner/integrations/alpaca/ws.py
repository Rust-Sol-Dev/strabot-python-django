from django.conf import settings

from stratbot.scanner.integrations.alpaca.bridges import AlpacaWebsocketBridge


websocket_uri = 'wss://stream.data.alpaca.markets/v2/sip'

# TODO: these are currently defined in bridges.py
base_subscriptions = {
    'trades': '{}',
    'quotes': '{}',
    'bars': '{}',
}

alpaca_ws = AlpacaWebsocketBridge(
    websocket_uri,
    settings.ALPACA_API_KEY,
    settings.ALPACA_API_SECRET,
    set(),
)
