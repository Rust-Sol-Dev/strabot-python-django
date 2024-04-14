import os
from collections import OrderedDict
import logging
from datetime import timedelta
from time import perf_counter
from collections import defaultdict
import pytz
from orjson import orjson

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings.dev")
import django
django.setup()

from stratbot.scanner.models.pricerecs import StockPriceRec, STOCK_TF_PRICEREC_MODEL_MAP
from stratbot.scanner.models.symbols import SymbolRec
from stratbot.scanner.models.timeframes import Timeframe


opens = defaultdict(dict[Timeframe, float])


def query_model(symbolrec):
    for tf in symbolrec.scan_timeframes:
        open = STOCK_TF_PRICEREC_MODEL_MAP[tf].objects.filter(symbol=symbolrec.symbol).latest().open
        opens[symbolrec.symbol][tf] = open


symbolrecs = SymbolRec.objects.filter(symbol_type='stock')
for symbolrec in symbolrecs:
    query_model(symbolrec)
