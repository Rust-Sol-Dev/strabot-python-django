from pathlib import Path
from time import perf_counter
from typing import Optional, Union
import logging

from django.core.cache import caches
from django.conf import settings
import pandas as pd


log = logging.getLogger(__name__)
cache = caches['markets']


def to_cache(*args, data):
    if len(args) == 0:
        raise ValueError('must provide at least one argument to cache_df')
    s = perf_counter()
    cache_key = ':'.join(str(arg) for arg in args)
    cache.set(cache_key, data, timeout=None)
    if isinstance(data, pd.DataFrame):
        log.debug(f'to_cache: wrote {len(data)} records to {cache_key} in {(perf_counter() - s) * 1000:0.4f} ms')
    else:
        log.debug(f'to_cache: wrote data to {cache_key} in {(perf_counter() - s) * 1000:0.4f} ms')


def from_cache(*args) -> Optional[Union[pd.DataFrame, list]]:
    s = perf_counter()
    cache_key = ':'.join(str(arg) for arg in args)
    data = cache.get(cache_key)
    if data is None:
        log.debug(f'from_cache: {cache_key} missing key or empty')
    else:
        log.debug(f'from_cache: read from {cache_key=} in {(perf_counter() - s) * 1000:0.4f} ms')
    return data


def _parquet_filepath(symbol_type: str, symbol: str, timeframe: str) -> Path:
    return Path(f'{settings.PARQUET_DIR}/{symbol_type}/{symbol}-{timeframe}.parquet')


def to_parquet(symbol_type: str, symbol: str, timeframe: str, df: pd.DataFrame):
    s = perf_counter()
    filepath = _parquet_filepath(symbol_type, symbol, timeframe)
    filepath.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(filepath)
    elapsed = perf_counter() - s
    log.info(f'to_parquet: completed in {elapsed * 1000:.4f} ms ({elapsed:.2f} s)')


def from_parquet(symbol_type: str, symbol: str, timeframe: str) -> Optional[pd.DataFrame]:
    s = perf_counter()
    filepath = _parquet_filepath(symbol_type, symbol, timeframe)
    try:
        df = pd.read_parquet(filepath)
    except FileNotFoundError:
        log.info(f'from_parquet: {filepath} not found')
        return None
    elapsed = perf_counter() - s
    log.info(f'from_parquet: completed in {elapsed * 1000:.4f} ms ({elapsed:.2f} s)')
    return df
