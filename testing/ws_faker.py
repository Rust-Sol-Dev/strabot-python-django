import sys
sys.path.append('/Users/TLK3/PycharmProjects/stratbot2')

import os
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings.dev")
import django
from django.utils import timezone
django.setup()

import asyncio
import random
from collections import deque
from time import perf_counter

from stratbot.scanner.models.symbols import SymbolRec
from ws import handle_timeframe_aggregate, _convert_to_historial_format, bulk_insert_bars


last_timestamp = int(timezone.now().timestamp() - 60 * 60 * 24 * 365)
bars = deque()


def generate_fake_bar(symbol):
    global last_timestamp
    s_timestamp = last_timestamp
    e_timestamp = s_timestamp + 60
    last_timestamp = last_timestamp + 60
    open_ = round(random.uniform(100, 200), 2)
    close = round(open_ + random.uniform(-1, 1), 2)
    high = round(max(open_, close) + random.uniform(0, 1), 2)
    low = round(min(open_, close) - random.uniform(0, 1), 2)
    vol = random.randint(1000, 10000)
    accumulated_vol = random.randint(10000, 20000)
    opening_price = round(open_ - random.uniform(-1, 1), 2)
    vwap = round((high + low + close) / 3, 2)
    vwap_today = round((high + low + close) / 3, 2)
    avg_trade_size = random.randint(100, 1000)

    return {
        's': s_timestamp,
        'e': e_timestamp,
        'o': open_,
        'h': high,
        'l': low,
        'c': close,
        'v': vol,
        'av': accumulated_vol,
        'op': opening_price,
        'vw': vwap,
        'a': vwap_today,
        'z': avg_trade_size,
        'sym': symbol,
    }


if __name__ == '__main__':
    symbols = SymbolRec.objects.filter(symbol_type='stock').values_list('symbol', flat=True)
    bars = [generate_fake_bar(symbol) for symbol in symbols]

    loop = asyncio.get_event_loop()
    tasks = [handle_timeframe_aggregate(bar) for bar in bars]
    results = loop.run_until_complete(asyncio.gather(*tasks))

    for result in results:
        bars.append(result)

    loop.run_until_complete(bulk_insert_bars(bars))
    # s = perf_counter()
    # tasks = [candle_to_cache_preprocess(bar) for bar in results]
    # loop.run_until_complete(asyncio.gather(*tasks))
    # print(f'{len(results)} writes completed in {(perf_counter() - s) * 1000:.4f} ms')
