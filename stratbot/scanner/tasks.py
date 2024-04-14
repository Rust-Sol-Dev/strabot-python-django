from __future__ import annotations
import logging
import pickle

import msgspec
import pandas as pd
import pytz
from celery import group, chain
from celery.canvas import Signature
from confluent_kafka import Producer
from django.conf import settings
from django.db.models import Q

from config import celery_app
from django.utils import timezone
from django.core.cache import caches

from .models.symbols import SymbolRec, SymbolType, Setup, Exchange
from .models.exchange_calendar import ExchangeCalendar
from .models.timeframes import Timeframe
from .ops import historical
from .integrations.binance.bridges import async_binance_bridge
from .ops.candles.metrics import atr_metrics


log = logging.getLogger(__name__)
cache = caches['markets']
r = cache.client.get_client(write=True)


@celery_app.task()
def add_new_symbol(symbol: str, symbol_type: str) -> None:
    symbol_type = SymbolType(symbol_type)
    exchange = Exchange.BINANCE if symbol_type == SymbolType.CRYPTO else Exchange.STOCK

    try:
        SymbolRec.objects.get(symbol=symbol, symbol_type=symbol_type)
    except SymbolRec.DoesNotExist:
        symbolrec = SymbolRec.objects.create(symbol=symbol, symbol_type=symbol_type, exchange=exchange)
        chained_tasks = chain(
            refresh_historical_db.si(symbolrec.pk, overwrite=True),
            refresh_company_meta.si(symbolrec.pk),
            refresh_candles_to_redis.si(symbolrec.pk),
            cache_historical_dfs.si(symbolrec.pk),
            refresh_metrics.si(symbolrec.pk),
            refresh_setups.si(symbolrec.pk)
        )
        chained_tasks()


@celery_app.task()
def queue_refresh_historical_db(symbol_type: str, overwrite: bool = False) -> None:
    symbol_type = SymbolType(symbol_type)
    signatures: list[Signature] = []
    for symbolrec in SymbolRec.objects.filter(symbol_type=symbol_type).iterator(chunk_size=2_000):
        symbol_signature = refresh_historical_db.si(symbolrec.pk, overwrite)
        signatures.append(symbol_signature)
    celery_group = group(signatures)
    celery_group.delay()


@celery_app.task()
def refresh_historical_db(symbolrec_pk: int, overwrite: bool = False) -> None:
    symbolrec = SymbolRec.objects.get(pk=symbolrec_pk)
    historical.refresh_historical_db(symbolrec, tf=Timeframe.MINUTES_1, overwrite=overwrite)


@celery_app.task()
def queue_refresh_redpanda(symbol_type: str) -> None:
    symbol_type = SymbolType(symbol_type)
    signatures: list[Signature] = []
    for symbolrec in SymbolRec.objects.filter(symbol_type=symbol_type).iterator(chunk_size=2_000):
        symbol_signature = refresh_candles_to_redpanda.si(symbolrec.pk)
        signatures.append(symbol_signature)
    celery_group = group(signatures)
    celery_group.delay()


def df_to_json(df: pd.DataFrame) -> pd.DataFrame:
    df = df.tail(5).reset_index()
    df['time'] = (df['time'].astype('int64') // 1e9)
    df = df[['time', 'open', 'high', 'low', 'close', 'volume', 'strat_id']]
    df.rename(columns={
        'time': 'ts',
        'open': 'o',
        'high': 'h',
        'low': 'l',
        'close': 'c',
        'volume': 'v',
        'strat_id': 'sid',
    }, inplace=True)
    return df


@celery_app.task()
def refresh_candles_to_redpanda(symbolrec_pk: int) -> None:
    conf = {
        'bootstrap.servers': settings.REDPANDA_BROKERS_STR,
        'security.protocol': settings.REDPANDA_SECURITY_PROTOCOL,
        'sasl.mechanism': settings.REDPANDA_SASL_MECHANISM,
        'sasl.username': settings.REDPANDA_USERNAME,
        'sasl.password': settings.REDPANDA_PASSWORD,
    }
    producer = Producer(**conf)

    symbolrec = SymbolRec.objects.get(pk=symbolrec_pk)
    for tf in symbolrec.scan_timeframes:
        df = getattr(symbolrec, symbolrec.TF_MAP[tf])
        if df is None or df.empty or len(df) < 2:
            continue
        df = df_to_json(df)
        bars_dict = df.to_dict(orient='records')
        payload = {'e': 'timescale', 'tf': str(tf), 'bars': bars_dict}
        json_value = msgspec.json.encode(payload)
        producer.produce('dispatch', key=symbolrec.symbol, value=json_value, callback=None)
    producer.flush(10)


@celery_app.task()
def queue_refresh_redis(symbol_type: str) -> None:
    symbol_type = SymbolType(symbol_type)
    signatures: list[Signature] = []
    for symbolrec in SymbolRec.objects.filter(symbol_type=symbol_type).iterator(chunk_size=2_000):
        symbol_signature = refresh_candles_to_redis.si(symbolrec.pk)
        signatures.append(symbol_signature)
    celery_group = group(signatures)
    celery_group.delay()


@celery_app.task()
def refresh_candles_to_redis(symbolrec_pk: int) -> None:
    symbolrec = SymbolRec.objects.get(pk=symbolrec_pk)

    ohlcv = {}
    opens = {}
    for tf in symbolrec.scan_timeframes:
        df = getattr(symbolrec, symbolrec.TF_MAP[tf])
        df = df_to_json(df)
        ohlcv[tf] = df.to_dict(orient='records')

        open_ = df.iloc[-1].o
        close = df.iloc[-1].c
        if close > open_:
            direction = 1
        elif close < open_:
            direction = -1
        else:
            direction = 0
        opens[tf] = direction

    r.json().set(f'barHistory:{symbolrec.symbol_type}:{symbolrec.symbol}', '$', ohlcv)
    r.json().set(f'TFC:{symbolrec.symbol_type}:{symbolrec.symbol}', '$', opens)


def parse_ohlcv_df(df: pd.DataFrame):
    df = df[['open', 'high', 'low', 'close', 'volume']]
    df.rename(columns={
        'open': 'o',
        'high': 'h',
        'low': 'l',
        'close': 'c',
        'volume': 'v',
    }, inplace=True)
    return df


AGG_DEFAULTS = {
    'o': 'first',
    'h': 'max',
    'l': 'min',
    'c': 'last',
    'v': 'sum',
}

RESAMPLE_PERIODS = {
    Timeframe.MINUTES_5: '5min',
    Timeframe.MINUTES_15: '15min',
    Timeframe.MINUTES_30: '30min',
    Timeframe.MINUTES_60: '60min',
    Timeframe.HOURS_4: '4h',
    Timeframe.HOURS_6: '6h',
    Timeframe.HOURS_12: '12h',
    Timeframe.WEEKS_1: 'W',
    Timeframe.MONTHS_1: 'MS',
    Timeframe.QUARTERS_1: 'QS',
    Timeframe.YEARS_1: 'YS',
}


def historical_resample(symbol_type: str, timeframe: Timeframe, df: pd.DataFrame = None):
    match timeframe:
        case Timeframe.MINUTES_60:
            kwargs = {}
            if symbol_type == SymbolType.STOCK:
                kwargs = {'offset': '30min'}
            df = df.resample('60min', **kwargs).agg(AGG_DEFAULTS).dropna()
        case Timeframe.HOURS_4:
            if symbol_type == SymbolType.STOCK:
                df.index = df.index.tz_convert(pytz.timezone("America/New_York"))
                df = df.resample('4h', offset='30min').agg(AGG_DEFAULTS).dropna()
                df.index = df.index.tz_convert(pytz.UTC)
            else:
                df = df.resample('4h').agg(AGG_DEFAULTS).dropna()
        case Timeframe.DAYS_1:
            pass
        case Timeframe.WEEKS_1:
            df_weekly = df.copy()
            df_weekly.index = df_weekly.index - pd.to_timedelta(df_weekly.index.dayofweek, unit='D')
            df = df_weekly.resample('W-MON').agg(AGG_DEFAULTS).dropna()
        case _:
            df = df.resample(RESAMPLE_PERIODS[timeframe]).agg(AGG_DEFAULTS).dropna()
    return df


@celery_app.task
def queue_refresh_historical_cache(symbol_type: str) -> None:
    symbol_type = SymbolType(symbol_type)
    signatures: list[Signature] = []
    for symbolrec in SymbolRec.objects.filter(symbol_type=symbol_type).iterator(chunk_size=2_000):
        symbol_signature = cache_historical_dfs.si(symbolrec.pk)
        signatures.append(symbol_signature)
    celery_group = group(signatures)
    celery_group.delay()


@celery_app.task
def cache_historical_dfs(symbolrec_pk: int) -> None:
    symbolrec = SymbolRec.objects.get(pk=symbolrec_pk)

    one_df = parse_ohlcv_df(symbolrec.one.tail(100_000).copy())
    serialized_df = pickle.dumps(one_df.tail(10_000))
    r.set(f'df:{symbolrec.symbol_type}:{symbolrec.symbol}:1', serialized_df)

    daily_df = parse_ohlcv_df(symbolrec.daily_db.copy())
    for tf in symbolrec.scan_timeframes:
        df = daily_df if tf >= Timeframe.DAYS_1 else one_df
        df = historical_resample(symbolrec.symbol_type, tf, df)
        serialized_df = pickle.dumps(df)
        r.set(f'df:{symbolrec.symbol_type}:{symbolrec.symbol}:{tf}', serialized_df)


# @celery_app.task()
# def flush_historical_stock_candle_cache() -> None:
#     """
#     Flushes the historical stock candle cache for all stock symbols and re-creates Setup records.
#     Polygon.io aggregates data from all exchanges after 12am EST, so we need to refresh historical
#     with full market coverage for accurate OHLC values.
#     """
#     refresh_historical_candles_cache.delay('stock', Timeframe.MINUTES_1, overwrite=True)
#     refresh_historical_candles_cache.delay('stock', Timeframe.DAYS_1, overwrite=True)
#     queue_refresh_metrics.delay('stock')
#     Setup.objects.filter(symbol_rec__symbol_type='stock',
#                          tf__in=['15', '30', '60', '4H', 'D', 'W'],
#                          expires__gt=timezone.now(),
#                          expires__lt=timezone.now() + timedelta(days=1)
#                          ).delete()
#     queue_setup_refresh.delay('stock')


# @celery_app.task()
# def refresh_stock_pricerec_caggs() -> None:
#     caggs = [
#         'stock_pricerec_15',
#         'stock_pricerec_30',
#         'stock_pricerec_12h',
#         'stock_pricerec_d',
#         'stock_pricerec_w',
#         'stock_pricerec_m',
#         'stock_pricerec_q',
#         'stock_pricerec_y',
#     ]
#     start_date = timezone.now().date() - timedelta(days=365 * 10)
#     with connection.cursor() as cursor:
#         for cagg in caggs:
#             query = f"""CALL refresh_continuous_aggregate('{cagg}', '{start_date}', NOW());"""
#             cursor.execute(query)


# @celery_app.task()
# def flush_historical_stock_pricerec() -> None:
#     """
#     Flushes the historical stock candle cache for all stock symbols and re-creates Setup records.
#     Polygon.io aggregates data from all exchanges after 12am EST, so we need to refresh historical
#     with full market coverage for accurate OHLC values.
#     """
#     yesterday_date = timezone.now().date() - timedelta(days=1)
#     StockPriceRec.objects.filter(time__gte=yesterday_date).delete()
#     queue_refresh_historical_db.delay('stock')
#     # refresh_stock_pricerec_caggs.delay()
#     queue_refresh_metrics.delay('stock')
#     Setup.objects.filter(symbol_rec__symbol_type='stock',
#                          tf__in=['15', '30', '60', '4H', 'D'],
#                          expires__gt=timezone.now(),
#                          ).delete()
#     queue_setup_refresh.delay('stock')


@celery_app.task()
def queue_setup_refresh(symbol_type: str) -> None:
    symbol_type = SymbolType(symbol_type)
    signatures: list[Signature] = []
    for symbolrec in SymbolRec.objects.filter(symbol_type=symbol_type).iterator(chunk_size=2_000):
        symbol_signature = refresh_setups.si(symbolrec.pk)
        signatures.append(symbol_signature)
    celery_group = group(signatures)
    # celery_group.delay()
    celery_group.apply_async(countdown=5)


@celery_app.task()
def refresh_setups(symbolrec_pk: int, timeframes: list[Timeframe] = None) -> None:
    symbolrec = SymbolRec.objects.get(pk=symbolrec_pk)
    # with transaction.atomic():
    #     historical.refresh_setups(symbolrec, timeframes=timeframes)
    historical.refresh_setups(symbolrec, tfs=timeframes)
    # historical.refresh_tfc(symbolrec)


@celery_app.task()
def queue_refresh_company_meta(symbol_type: str) -> None:
    symbol_type = SymbolType(symbol_type)
    signatures: list[Signature] = []
    for symbolrec in SymbolRec.objects.filter(symbol_type=symbol_type).iterator(chunk_size=2_000):
        symbol_signature = refresh_company_meta.si(symbolrec.pk)
        signatures.append(symbol_signature)
    celery_group = group(signatures)
    celery_group.delay()


@celery_app.task()
def refresh_company_meta(symbolrec_pk: int) -> None:
    symbolrec = SymbolRec.objects.get(pk=symbolrec_pk)
    historical.refresh_yfinance_meta(symbolrec)


# @celery_app.task()
# def refresh_crypto_quotes():
#     historical.refresh_crypto_quotes()


@celery_app.task()
def refresh_crypto_symbols(min_volume_usd: float = 100_000_000) -> None:
    symbolrecs = []
    symbols = async_binance_bridge.markets_by_volume(min_volume_usd=min_volume_usd)
    quotes = async_binance_bridge.quotes()
    for symbol in symbols:
        price = quotes[symbol]
        symbolrecs.append(SymbolRec(symbol=symbol, symbol_type=SymbolType.CRYPTO, price=price, as_of=timezone.now()))
    SymbolRec.objects.bulk_create(symbolrecs, ignore_conflicts=True)
    SymbolRec.objects.filter(symbol_type='crypto', symbol__in=symbols).update(skip_discord_alerts=False)
    SymbolRec.objects.filter(~Q(symbol__in=symbols), symbol_type='crypto').update(skip_discord_alerts=True)


@celery_app.task()
def refresh_market_schedule_cache() -> None:
    exchange_calendar = ExchangeCalendar()
    exchange_calendar.cache_schedules()


# @celery_app.task()
# def purge_crypto_symbols(symbols: list) -> None:
#     for symbolrec in SymbolRec.objects.filter(symbol__in=symbols):
#         Setup.objects.filter(symbol_rec=symbolrec).delete()
#     SymbolRec.objects.filter(symbol__in=symbols).delete()


# @celery_app.task()
# def queue_refresh_tfc(symbol_type: str) -> None:
#     signatures: list[Signature] = []
#     for symbolrec in SymbolRec.objects.filter(symbol_type=symbol_type).iterator(chunk_size=2_000):
#         tfc_signature = refresh_tfc.si(symbolrec.pk)
#         signatures.append(tfc_signature)
#     celery_group = group(signatures)
#     celery_group.delay()


# @celery_app.task()
# def refresh_tfc(symbolrec_pk: int) -> None:
#     symbolrec = SymbolRec.objects.get(pk=symbolrec_pk)
#     historical.refresh_tfc(symbolrec)


@celery_app.task()
def queue_refresh_metrics(symbol_type: str) -> None:
    signatures: list[Signature] = []
    for symbolrec in SymbolRec.objects.filter(symbol_type=symbol_type).iterator(chunk_size=2_000):
        metrics_signature = refresh_metrics.si(symbolrec.pk)
        signatures.append(metrics_signature)
    celery_group = group(signatures)
    celery_group.delay()


@celery_app.task()
def refresh_metrics(symbolrec_pk: int) -> None:
    symbolrec = SymbolRec.objects.get(pk=symbolrec_pk)
    df = getattr(symbolrec, symbolrec.TF_MAP[Timeframe.DAYS_1])
    atr, atr_percentage = atr_metrics(df)
    symbolrec.atr = atr
    symbolrec.atr_percentage = atr_percentage
    # symbolrec.volume = symbolrec.todays_volume
    # symbolrec.tfc = {k: asdict(v) for k, v in symbolrec.tfc_state().items()}
    symbolrec.save()
