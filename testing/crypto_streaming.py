import os
import sys
import asyncio
import logging
from time import perf_counter
from datetime import datetime, timedelta

sys.path.append('/Users/TLK3/PycharmProjects/stratbot2')
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings.dev")
import django
from django.conf import settings
from django.core.cache import caches
from asgiref.sync import async_to_sync, sync_to_async
django.setup()

from cryptofeed import FeedHandler
from cryptofeed.backends.postgres import CandlesPostgres
# from cryptofeed.backends.aggregate import OHLCV
from cryptofeed.defines import TRADES, CANDLES
from cryptofeed.exchanges import BinanceFutures, Bybit
from arcticdb import Arctic

from stratbot.scanner.models.timeframes import Timeframe
from stratbot.scanner.integrations.polygon.bridges_old import AsyncPolygonBridge


# postgres_cfg = {'host': '127.0.0.1', 'user': 'testing', 'db': 'testing', 'pw': 'testing'}
# postgres_cfg = {'host': '143.244.158.147', 'db': 'stratbot', 'user': 'stratbot_pg_user', 'pw': 'stratbot_pg_pw'}


logger = logging.getLogger(__name__)
logger.level = logging.DEBUG


# ac = Arctic(settings.ARCTIC_DB_URI)
# library = ac.get_library('crypto', create_if_missing=True)


def nbbo_update(symbol, bid, bid_size, ask, ask_size, bid_feed, ask_feed):
    print(
        f'Pair: {symbol} Bid Price: {bid:.2f} Bid Size: {bid_size:.6f} Bid Feed: {bid_feed} Ask Price: {ask:.2f} Ask Size: {ask_size:.6f} Ask Feed: {ask_feed}')


async def trade(t, receipt_ts):
    print(t)


async def candle(c, receipt_ts):
    print(f'{receipt_ts}: ', c.symbol, c.exchange, c.timestamp, c.start, c.stop, c.open, c.high, c.low, c.close, c.volume)


column_mappings = {
    'exchange': 'exchange',
    'symbol': 'symbol',
    'open': 'open',
    'high': 'high',
    'low': 'low',
    'close': 'close',
    'volume': 'volume',
    #'timestamp': 'time',
    'start': 'time',
    # 'stop': 'stop',
    # 'closed': 'closed',
}


def main():
    f = FeedHandler()
    # f.add_feed(Coinbase(symbols=['BTC-USD'], channels=[CANDLES], callbacks={TRADES: trade}))
    # f.add_feed(Kraken(symbols=['BTC-USD'], channels=[CANDLES], callbacks={TRADES: trade}))
    # f.add_feed(Gemini(symbols=['BTC-USD'], channels=[CANDLES], callbacks={TRADES: trade}))
    # f.add_feed(Binance(symbols=['BTC-USDT'], channels=[TRADES], callbacks={TRADES: trade}))

    # f.add_feed(Gemini(symbols=['BTC-USD', 'ETH-USD'], channels=[CANDLES], callbacks={CANDLES: candle}))
    # f.add_feed(Kraken(symbols=['BTC-USD', 'ETH-USD'], channels=[CANDLES], callbacks={CANDLES: candle}))

    symbols = [
        'BTC-USDT-PERP', 'ETH-USDT-PERP', 'XRP-USDT-PERP', 'BNB-USDT-PERP', 'SOL-USDT-PERP', 'LTC-USDT-PERP',
        'DOGE-USDT-PERP', 'MATIC-USDT-PERP', 'ARB-USDT-PERP', 'LINK-USDT-PERP', 'XLM-USDT-PERP', 'AVAX-USDT-PERP',
        'ADA-USDT-PERP', 'DOT-USDT-PERP', 'OP-USDT-PERP', 'APT-USDT-PERP',
    ]

    # f.add_feed(BinanceFutures(symbols=symbols, channels=[CANDLES], callbacks={
    #    CANDLES: CandlesPostgres(**postgres_cfg, custom_columns=column_mappings, table='crypto_pricerec_1')}))
    # f.add_feed(Bybit(symbols=symbols, channels=[CANDLES], callbacks={
    #    CANDLES: CandlesPostgres(**postgres_cfg, custom_columns=column_mappings, table='crypto_pricerec_1')}))

    f.add_feed(BinanceFutures(symbols=symbols, channels=[CANDLES], callbacks={CANDLES: candle}))
    f.add_feed(Bybit(symbols=symbols, channels=[CANDLES], callbacks={CANDLES: candle}))

    # f.add_feed(Binance(symbols=['BTC-USDT'], channels=[CANDLES], callbacks={CANDLES: candle}))
    # f.add_feed(Bybit(symbols=['BTC-USDT-PERP', 'ETH-USDT-PERP'], channels=[CANDLES], callbacks={CANDLES: candle}))
    # f.add_feed(Binance(channels=[CANDLES], symbols=['BTC-USDT'], callbacks={CANDLES: candle}))
    # f.add_feed(OKX(symbols=['BTC-USDT-PERP', 'ETH-USDT-PERP'], channels=[CANDLES], callbacks={CANDLES: candle}))
    # f.add_feed(Phemex(symbols=['BTC-USDT-PERP', 'ETH-USDT-PERP'], channels=[CANDLES], callbacks={CANDLES: candle}))

    # The following feed shows custom_columns and uses the custom_candles table example from the bottom of postgres_tables.sql. Obviously you can swap this out for your own table layout, just update the dictionary above
    # f.add_feed(Binance(channels=[CANDLES], symbols=['FTM-USDT'], callbacks={
    #     CANDLES: CandlesPostgres(**postgres_cfg, custom_columns=column_mappings, table='custom_candles')}))

    # f.add_nbbo([Coinbase, Kraken, Gemini], ['BTC-USD'], nbbo_update)
    f.run()


async def query_historical(symbols: set, timeframe: str):
    s = perf_counter()
    async with AsyncPolygonBridge(settings.POLYGON_API_KEY) as client:
        if timeframe == Timeframe.DAYS_1:
            start_date = datetime(2005, 1, 1)
        else:
            start_date = datetime.now() - timedelta(days=1)
        tasks = []
        for symbol in symbols:
            # results = await client.historical_new(symbol, start_date=start_date, tf=timeframe)
            # print(symbol, results)
            tasks.append(client.historical(symbol, start_date=start_date, tf=timeframe))
        results = await asyncio.gather(*tasks)
        # results = await client.historical(symbols, start_date=start_date, tf=timeframe)
    elapsed = perf_counter() - s
    logger.info(f'polygon.io: {len(symbols)} symbols queried in {elapsed * 1000:.4f} ms ({elapsed:.2f} s)')
    return results


def initialize(symbols: set):
    timeframe = Timeframe.MINUTES_1
    historical_candles = asyncio.run(query_historical(symbols, timeframe=timeframe))
    for symbol, df in historical_candles:
        # library.write(f'{symbol}:{timeframe}', df)
        print(f'{symbol} [{timeframe}] - {len(df)} records written to ArcticDB')


if __name__ == '__main__':
    # initialize()
    main()
