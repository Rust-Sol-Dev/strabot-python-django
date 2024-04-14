from __future__ import annotations

from .clients import client, async_client
from ..ccxt.bridges import CcxtBridge


class MexcBridge(CcxtBridge):
    def __init__(self, client):
        super().__init__(client)

    def markets_by_volume(self, min_usd: int = 25_000_000) -> list:
        excludes = ['USDT/USD', 'USDT-PERP']
        symbols = []
        for _, data in self.markets().items():
            symbol = data['info']['symbol']
            if float(data['info']['quoteVolume']) > min_usd and symbol not in excludes and symbol.endswith('USDT'):
                symbols.append(symbol)
        return symbols


class AsyncMexcBridge:
    def __init__(self, client: async_client):
        self.client = client


mexc_bridge = MexcBridge(client)
async_mexc_bridge = AsyncMexcBridge(async_client)
