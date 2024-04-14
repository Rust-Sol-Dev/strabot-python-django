from alpaca.data.historical import StockHistoricalDataClient

from django.conf import settings


client = StockHistoricalDataClient(settings.ALPACA_API_KEY, settings.ALPACA_API_SECRET)
