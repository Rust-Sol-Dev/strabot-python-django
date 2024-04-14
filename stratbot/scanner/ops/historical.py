from __future__ import annotations

from datetime import datetime, timedelta, time
from itertools import chain

import logging
from dataclasses import asdict
from time import perf_counter

import pandas as pd
import pytz
# from polygon import exceptions as polygon_exceptions
from django.utils import timezone
import yfinance as yf

from ..models.symbols import SymbolRec, SymbolType, Setup, ProviderMeta
from ..models.exchange_calendar import ExchangeCalendar
from ..models.pricerecs import StockPriceRec, CryptoPriceRec
from ..models.timeframes import Timeframe
# from ..integrations.polygon.bridges import polygon_bridge, async_polygon_bridge
# from ..integrations.twelvedata.bridges import twelvedata_bridge
from ..integrations.alpaca.bridges import alpaca_bridge
from ..integrations.binance.bridges import async_binance_bridge, binance_bridge


log = logging.getLogger(__name__)


def df_to_pricerec(symbol_type: str, df: pd.DataFrame):
    """
    DataFrames returned from client bridges have a DatetimeIndex. This must be removed
    to write the column to the database. Also drop the 'timestamp' field
    """
    if df is None or (isinstance(df, pd.DataFrame) and df.empty):
        return
    df = df.copy()
    df.reset_index(inplace=True)

    try:
        del df['timestamp']
    except KeyError:
        pass

    try:
        model = StockPriceRec if symbol_type == SymbolType.STOCK else CryptoPriceRec
        model_instances = [model(**row) for row in df.to_dict('records')]
        model.objects.bulk_create(model_instances, ignore_conflicts=True)
    except Exception as e:
        log.error(f"error writing to db: {e}")


def backfill_db(symbolrec: SymbolRec):
    schedule_offsets = {
        Timeframe.DAYS_1: (0, -14),
        Timeframe.MINUTES_1: (-13, -1),
    }

    for tf in (Timeframe.DAYS_1, Timeframe.MINUTES_1):
        if symbolrec.symbol_type == SymbolType.STOCK:
            start_delta = timedelta(days=365 * 10)
            schedule_start_date = timezone.now() - start_delta
            schedule_end_date = timezone.now()
            exchange = ExchangeCalendar(start_date=schedule_start_date, end_date=schedule_end_date)
            schedule = exchange.schedule(Timeframe.DAYS_1, from_cache=False)
            start_date = schedule.iloc[schedule_offsets[tf][0]].market_open
            end_date = schedule.iloc[schedule_offsets[tf][1]].market_close - timedelta(minutes=1)
            df = alpaca_bridge.historical(symbolrec.symbol, tf, start_date, end_date)
        else:
            schedule_start_date = timezone.now().date() - timedelta(days=365 * 5)
            schedule_end_date = timezone.now().date()
            schedule = pd.date_range(start=schedule_start_date, end=schedule_end_date, freq='D', tz='UTC')
            start_date = schedule[schedule_offsets[tf][0]]
            end_date = schedule[schedule_offsets[tf][1]] + timedelta(days=1)
            df = binance_bridge.historical(symbolrec.symbol, tf, start_date)

        if isinstance(df, pd.DataFrame) and not df.empty:
            df_to_pricerec(symbolrec.symbol_type, df)
            logging.info(f'backfilled {symbolrec.symbol} [{tf}] from {start_date} to {end_date}')
        else:
            logging.warning(f'no data for {symbolrec.symbol} [{tf}] from {start_date} to {end_date}')


def refresh_historical_db(symbolrec: SymbolRec, tf: Timeframe, overwrite: bool = False) -> None:
    symbol_type = symbolrec.symbol_type
    model = StockPriceRec if symbol_type == SymbolType.STOCK else CryptoPriceRec

    if overwrite:
        model.objects.filter(symbol=symbolrec.symbol).delete()
        backfill_db(symbolrec)
        return

    try:
        start_date = model.objects.filter(symbol=symbolrec.symbol).latest().time
    except (model.DoesNotExist, AttributeError) as e:
        backfill_db(symbolrec)
        return

    if symbol_type == SymbolType.STOCK:
        end_date = timezone.now()
        df = alpaca_bridge.historical(symbolrec.symbol, tf, start_date, end_date)
    else:
        # TODO: this will need to be handled differently to support multiple exchanges
        df = binance_bridge.historical(symbolrec.symbol, tf, start_date)

    df_to_pricerec(symbolrec.symbol_type, df)


def refresh_crypto_quotes():
    symbolrecs = SymbolRec.objects.filter(symbol_type=SymbolType.CRYPTO)
    quotes = async_binance_bridge.quotes()
    for symbolrec in symbolrecs:
        try:
            symbolrec.price = quotes[symbolrec.symbol]
            symbolrec.as_of = timezone.now()
        except KeyError:
            pass
    SymbolRec.objects.bulk_update(symbolrecs, ['price', 'as_of'])
    return quotes


# def refresh_tfc(symbolrec: SymbolRec) -> None:
#     s = perf_counter()
#     symbolrec.tfc = {k: asdict(v) for k, v in symbolrec.tfc_state(['D', 'W', 'M', 'Q', 'Y']).items()}
#     symbolrec.save()
#     log.info(f'write TFC for {symbolrec.symbol} in {(perf_counter() - s) * 1000:.4f} ms')


# def refresh_polygon_meta(symbolrec: SymbolRec):
#     log.info(f'{symbolrec.symbol}: updating metadata')
#     try:
#         meta = polygon_bridge.client.get_ticker_details(symbolrec.symbol)
#         symbolrec.polygon_meta = asdict(meta)
#         symbolrec.save()
#     except polygon_exceptions.BadResponse as e:
#         log.error(f'{symbolrec.symbol}: {e}')


def refresh_yfinance_meta(symbolrec: SymbolRec):
    log.info(f'{symbolrec.symbol}: updating metadata')
    try:
        tickers = yf.Tickers(symbolrec.symbol)
        meta = tickers.tickers[symbolrec.symbol].info
        try:
            provider = symbolrec.provider_metas.get(name='yfinance')
            provider.meta = meta
            provider.last_updated = timezone.now()
            symbolrec.save()
        except ProviderMeta.DoesNotExist:
            symbolrec.provider_metas.create(name='yfinance', meta=meta)
    except KeyError:
        log.error(f'meta missing for [{symbolrec.symbol} on YFinance')


# def refresh_stock_minute_candles():
#     snapshot = polygon_bridge.snapshot()
#     try:
#         minute_candles_by_symbol = {rec['ticker']: rec['min'] for rec in snapshot['tickers']}
#     except AttributeError:
#         log.error(f'error parsing snapshot: {snapshot}')
#         return
#
#     new_pricerecs = []
#     symbolrecs = SymbolRec.objects.filter(symbol_type=SymbolType.STOCK)
#     for symbolrec in symbolrecs:
#         if candle := minute_candles_by_symbol.get(symbolrec.symbol):
#             new_pricerecs.append(StockPriceRec(
#                 time=datetime.fromtimestamp(candle['t'] / 1000, tz=pytz.UTC),
#                 symbol=symbolrec.symbol,
#                 # exchange='POLYGON',
#                 open=candle['o'],
#                 high=candle['h'],
#                 low=candle['l'],
#                 close=candle['c'],
#                 volume=candle['v'],
#                 transactions=candle['n'],
#                 vwap=candle['vw'],
#             ))
#     StockPriceRec.objects.bulk_create(new_pricerecs, ignore_conflicts=True)
#     log.info(f'wrote {len(new_pricerecs)} minute candles')


def refresh_setups(symbolrec: SymbolRec, tfs: list[Timeframe] = None) -> None:
    if tfs is None:
        tfs = symbolrec.scan_timeframes

    skip_timeframes = (
        Setup.objects
        .filter(symbol_rec=symbolrec, expires__gt=timezone.now())
        .values_list('tf', flat=True)
        .distinct()
    )
    timeframes_to_scan = set(tfs) - set(skip_timeframes)
    if not timeframes_to_scan:
        log.info(f'{symbolrec.symbol}: unexpired setups found for all timeframes, skipping')
        return

    to_scan_log = [str(tf) for tf in timeframes_to_scan]
    log.info(f'{symbolrec.symbol}: scanning {to_scan_log}, skipping {list(skip_timeframes)}')
    if setups := symbolrec.scan_strat_setups(timeframes_to_scan):
        setups_to_db = list(chain(*setups.values()))
        Setup.objects.bulk_create(setups_to_db, ignore_conflicts=True)
        timeframes_written = [str(tf) for tf in setups.keys()]
        log.info(f'{symbolrec.symbol}: {timeframes_written} written to db')
