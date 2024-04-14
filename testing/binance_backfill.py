from datetime import datetime, timedelta

import pandas as pd
import requests

from stratbot.scanner.models.symbols import SymbolRec


def binance_backfill(symbolrec: SymbolRec):
    symbol = symbolrec.symbol
    data_url = f'https://data.binance.vision/?prefix=data/futures/um/daily/klines'

    requests.get()
    url = data_url + f'/{symbol}/1m/{symbol}-1m-{date}'

    'https://data.binance.vision/data/futures/um/daily/klines/BTCUSDT/1m/BTCUSDT-1m-2023-11-02.zip'
    data_url.format(symbolrec.symbol, tf)
