from stratbot.scanner.integrations.binance.bridges import BinanceWebsocketBridge


websocket_uri = 'wss://fstream.binance.com/ws'
subscriptions = {
    '!ticker@arr',
    '!markPrice@arr@1s',
    '!forceOrder@arr',
    '{}_perpetual@continuousKline_1m',
    '{}@aggTrade',
    # '{}@trade',
}
binance_ws = BinanceWebsocketBridge(websocket_uri, subscriptions)
