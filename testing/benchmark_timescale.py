import asyncio
import concurrent.futures
from datetime import timedelta, datetime
from time import perf_counter
from collections import defaultdict

from stratbot.scanner.models.pricerecs import STOCK_TF_PRICEREC_MODEL_MAP
from stratbot.scanner.models.symbols import SymbolRec
from stratbot.scanner.models.timeframes import Timeframe

timedeltas = {
    Timeframe.MINUTES_15: timedelta(hours=6),
    Timeframe.MINUTES_30: timedelta(hours=12),
    Timeframe.MINUTES_60: timedelta(days=1),
    Timeframe.HOURS_4: timedelta(days=10),
    Timeframe.DAYS_1: timedelta(days=30),
    Timeframe.WEEKS_1: timedelta(days=12),
    Timeframe.MONTHS_1: timedelta(weeks=52),
    Timeframe.QUARTERS_1: timedelta(weeks=52 * 3),
    Timeframe.YEARS_1: timedelta(weeks=52 * 5),
}
all_data = defaultdict(dict)


def get_data(symbolrec):
    for tf in symbolrec.scan_timeframes:
        model = STOCK_TF_PRICEREC_MODEL_MAP[tf]
        start_date = datetime.utcnow().astimezone() - timedeltas[tf]
        data = (model.df
                .filter(symbol=symbolrec.symbol, bucket__gte=start_date)
                .to_timeseries(index='bucket')
                )
        # print(symbolrec.symbol, tf)
        all_data[symbolrec.symbol][tf] = data


s = perf_counter()
symbolrecs = SymbolRec.objects.filter(symbol_type='stock', exchange='POLYGON')
with concurrent.futures.ThreadPoolExecutor() as executor:
    executor.map(get_data, symbolrecs)
# for symbolrec in symbolrecs:
#     get_data(symbolrec)
elapsed = perf_counter() - s
print(f'completed in {elapsed * 1000:.4f} ms ({elapsed:.2f} s)')

# ===============================================================

import concurrent.futures
from collections import defaultdict

all_data = defaultdict(dict)

tfs = (
    Timeframe.MINUTES_15,
    Timeframe.MINUTES_30,
    Timeframe.MINUTES_60,
    Timeframe.HOURS_4,
    Timeframe.DAYS_1,
    Timeframe.WEEKS_1,
    Timeframe.MONTHS_1,
    Timeframe.QUARTERS_1,
)

def get_data(symbolrec):
    for tf in symbolrec.scan_timeframes:
    # for tf in tfs:
        tf_df = symbolrec.historical(tf)
        all_data[symbolrec.symbol][tf] = tf_df

s = perf_counter()
symbolrecs = SymbolRec.objects.filter(symbol_type='stock', exchange='POLYGON')
with concurrent.futures.ThreadPoolExecutor() as executor:
    executor.map(get_data, symbolrecs)
# for symbolrec in symbolrecs:
#     get_data(symbolrec)
elapsed = perf_counter() - s
print(f'completed in {elapsed * 1000:.4f} ms ({elapsed:.2f} s)')