import json

from channels.generic.websocket import AsyncWebsocketConsumer

from .serializers import SymbolRecSerializer
from .models.symbols import SymbolRec


class SymbolRecConsumer(AsyncWebsocketConsumer):
    async def connect(self):
        await self.accept()

    async def disconnect(self, close_code):
        pass

    async def receive(self, text_data):
        data = json.loads(text_data)
        action = data.get('action')
        symbol = data.get('symbol')

        if action == 'subscribe':
            await self.subscribe(symbol)

    async def subscribe(self, symbol):
        group_name = f'symbol_{symbol}'
        await self.channel_layer.group_add(group_name, self.channel_name)

    async def send_price_data(self, event):
        price = event['price']
        symbol = event['symbol']
        await self.send(text_data=json.dumps({'symbol': symbol, 'price': price}))
