from __future__ import annotations

import logging
import pickle
import uuid
from dataclasses import dataclass, asdict
from functools import cached_property
from typing import Final, Union, Optional
from decimal import Decimal, ROUND_HALF_UP
from datetime import timedelta, datetime

import redis
from dateutil.relativedelta import relativedelta

import pandas as pd
import pytz
from dirtyfields import DirtyFieldsMixin
from django.db import models
from django.utils import timezone
from django.contrib.postgres.fields import ArrayField
from django.conf import settings
from django.core.cache import caches

from stratbot.scanner.ops.candles import metrics
from .exchange_calendar import ExchangeCalendar, MARKET_TIMEZONE
from .pricerecs import (
    CryptoPriceRec, CRYPTO_TF_PRICEREC_MODEL_MAP,
    StockPriceRec, STOCK_TF_PRICEREC_MODEL_MAP,
    StockPriceRecViewD, CryptoPriceRecViewD,
)
from .timeframes import Timeframe, TIMEFRAMES_INTRADAY
from v1.perf import func_timer
from ..ops.candles.candlepair import CandlePair

logger = logging.getLogger(__name__)


cache = caches['markets']
r = cache.client.get_client(write=True)
# r = redis.Redis(host=settings.REDIS_HOST, port=6379, db=11)
# r = redis.Redis(host='127.0.0.1', port=6379, db=11)


class SymbolType(models.TextChoices):
    STOCK = "stock", "Stock"
    CRYPTO = "crypto", "Crypto"


class SymbolTypeManager:
    VALID_TIMEFRAMES: Final[dict[str, list[Timeframe]]] = {
        SymbolType.STOCK: list(map(Timeframe, ['1', '5', '15', '30', '60', '4H', 'D', 'W', 'M', 'Q', 'Y'])),
        SymbolType.CRYPTO: list(map(Timeframe, ['1', '5', '15', '30', '60', '4H', '6H', '12H', 'D', 'W', 'M', 'Q', 'Y'])),
    }

    SCAN_TIMEFRAMES: Final[dict[str, list[Timeframe]]] = {
        SymbolType.STOCK: list(map(Timeframe, ['15', '30', '60', '4H', 'D', 'W', 'M', 'Q', 'Y'])),
        SymbolType.CRYPTO: list(map(Timeframe, ['15', '30', '60', '4H', '6H', '12H', 'D', 'W', 'M', 'Q', 'Y'])),
    }

    DISPLAY_TIMEFRAMES: Final[dict[str, list[Timeframe]]] = {
        SymbolType.STOCK: list(map(Timeframe, ['60', '4H', 'D', 'W', 'M', 'Q', 'Y'])),
        SymbolType.CRYPTO: list(map(Timeframe, ['60', '4H', '6H', '12H', 'D', 'W', 'M', 'Q', 'Y'])),
    }

    TFC_TIMEFRAMES: Final[dict[str, list[Timeframe]]] = {
        SymbolType.STOCK: list(map(Timeframe, ['D', 'W', 'M', 'Q', 'Y'])),
        SymbolType.CRYPTO: list(map(Timeframe, ['D', 'W', 'M', 'Q', 'Y'])),
    }

    INTRADAY_TIMEFRAMES: Final[dict[str, list[Timeframe]]] = {
        SymbolType.STOCK: list(map(Timeframe, ['15', '30', '60', '4H'])),
        SymbolType.CRYPTO: list(map(Timeframe, ['15', '30', '60', '4H', '6H', '12H'])),
    }

    DAILY_TIMEFRAMES: Final[dict[str, list[Timeframe]]] = {
        SymbolType.STOCK: list(map(Timeframe, ['D', 'W', 'M', 'Q', 'Y'])),
        SymbolType.CRYPTO: list(map(Timeframe, ['D', 'W', 'M', 'Q', 'Y'])),
    }

    @classmethod
    def scan_timeframes(cls, symbol_type: SymbolType) -> list[Timeframe]:
        return cls.SCAN_TIMEFRAMES.get(symbol_type, list())

    @classmethod
    def display_timeframes(cls, symbol_type: SymbolType) -> list[Timeframe]:
        return cls.DISPLAY_TIMEFRAMES.get(symbol_type, list())

    @classmethod
    def tfc_timeframes(cls, symbol_type: SymbolType) -> list[Timeframe]:
        return cls.TFC_TIMEFRAMES.get(symbol_type, list())

    @classmethod
    def intraday_timeframes(cls, symbol_type) -> list[Timeframe]:
        return cls.INTRADAY_TIMEFRAMES.get(symbol_type, list())

    @classmethod
    def daily_timeframes(cls, symbol_type) -> list[Timeframe]:
        return cls.DAILY_TIMEFRAMES.get(symbol_type, list())

    @classmethod
    def valid_timeframes(cls, symbol_type: SymbolType) -> list[Timeframe]:
        return cls.VALID_TIMEFRAMES.get(symbol_type, list())


class Exchange(models.TextChoices):
    NYSE = "NYSE", "NYSE"
    POLYGON = "POLYGON", "Polygon"
    BINANCE = "BINANCE", "Binance"
    STOCK = "STOCK", "STOCK"


class StockIndex(models.Model):
    symbol = models.CharField("Symbol", max_length=5)
    name = models.CharField("Index Name", max_length=32)

    def __str__(self):
        return f'{self.symbol} | {self.name}'


class SymbolRec(models.Model):
    TF_MAP = {
        '1': 'one',
        '5': 'five',
        '15': 'fifteen',
        '30': 'thirty',
        '60': 'hourly',
        '4H': 'four_hour',
        '6H': 'six_hour',
        '12H': 'twelve_hour',
        'D': 'daily',
        'W': 'weekly',
        'M': 'monthly',
        'Q': 'quarterly',
        'Y': 'yearly',
    }

    AGG_DEFAULTS = {
        'symbol': 'first',
        # 'exchange': 'first',
        'open': 'first',
        'high': 'max',
        'low': 'min',
        'close': 'last',
        'volume': 'sum',
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

    TRADE_DIRECTIONS = {
        '2D': [1],
        '2U': [-1],
        '1': [1, -1]
    }

    CANDLE_EXPIRE_DELTAS = {
        '5': timedelta(minutes=5),
        '15': timedelta(minutes=15),
        '30': timedelta(minutes=30),
        '60': timedelta(hours=1),
        '4H': timedelta(hours=4),
        '6H': timedelta(hours=6),
        '12H': timedelta(hours=12),
        'D': timedelta(days=1),
        'W': timedelta(weeks=1),
        'M': relativedelta(months=1),
        'Q': relativedelta(months=3),
        'Y': relativedelta(years=1),
    }

    HISTORICAL_TIMEDELTAS = {
        Timeframe.MINUTES_5: timedelta(days=30),
        Timeframe.MINUTES_15: timedelta(days=30),
        Timeframe.MINUTES_30: timedelta(days=30),
        Timeframe.MINUTES_60: timedelta(days=30),
        Timeframe.HOURS_4: timedelta(days=30),
        Timeframe.HOURS_6: timedelta(days=30),
        Timeframe.HOURS_12: timedelta(days=30),
        Timeframe.DAYS_1: timedelta(weeks=52 * 3),
        Timeframe.WEEKS_1: timedelta(weeks=52 * 3),
        Timeframe.MONTHS_1: timedelta(weeks=52 * 3),
        Timeframe.QUARTERS_1: timedelta(weeks=52 * 3),
        Timeframe.YEARS_1: timedelta(weeks=52 * 10),
    }

    exchange = models.CharField("Exchange", max_length=32, choices=Exchange.choices)
    symbol = models.CharField("Symbol", max_length=32, unique=True)
    symbol_type = models.CharField("Type", max_length=7, choices=SymbolType.choices)
    price = models.FloatField("Price", null=True, blank=True)
    atr = models.FloatField("ATR $", null=True, blank=True)
    atr_percentage = models.FloatField("ATR %", null=True, blank=True)
    # tfc = models.JSONField("Timeframe Continuity", null=True, blank=True)
    as_of = models.DateTimeField("As Of", null=True, blank=True)
    stock_indexes = models.ManyToManyField(StockIndex, blank=True)
    is_index = models.BooleanField("Is Index", default=False)
    is_etf = models.BooleanField("Is ETF", default=False)
    is_sector = models.BooleanField('Is Sector', default=False)
    skip_discord_alerts = models.BooleanField("Skip Discord Alerts", default=False)
    # TODO: this needs to be fixed for crypto, default is stocks
    exchange_calendar = ExchangeCalendar()

    class Meta:
        verbose_name = "Symbol Record"
        verbose_name_plural = "Symbol Records"
        constraints = [
            models.UniqueConstraint(fields=['exchange', 'symbol'], name='unique_exchange_symbol')
        ]

    def __str__(self):
        return self.symbol

    @property
    def valid_timeframes(self) -> list[Timeframe]:
        return SymbolTypeManager.valid_timeframes(SymbolType(self.symbol_type))

    @property
    def scan_timeframes(self) -> list[Timeframe]:
        return SymbolTypeManager.scan_timeframes(SymbolType(self.symbol_type))

    @property
    def display_timeframes(self) -> list[Timeframe]:
        return SymbolTypeManager.display_timeframes(SymbolType(self.symbol_type))

    @property
    def tfc_timeframes(self) -> list[Timeframe]:
        return SymbolTypeManager.tfc_timeframes(SymbolType(self.symbol_type))

    @property
    def intraday_timeframes(self) -> list[Timeframe]:
        return SymbolTypeManager.intraday_timeframes(SymbolType(self.symbol_type))

    @property
    def tf_pricerec_model_map(self):
        return STOCK_TF_PRICEREC_MODEL_MAP if self.symbol_type == SymbolType.STOCK else CRYPTO_TF_PRICEREC_MODEL_MAP

    @property
    def is_crypto(self):
        return self.symbol_type == SymbolType.CRYPTO

    @property
    def is_stock(self):
        return self.symbol_type == SymbolType.STOCK

    def historical_resample(self, timeframe: Timeframe, df: pd.DataFrame):
        match timeframe:
            case Timeframe.MINUTES_60:
                kwargs = {}
                if self.symbol_type == SymbolType.STOCK:
                    kwargs = {'offset': '30min'}
                df = df.resample('60min', **kwargs).agg(self.AGG_DEFAULTS).dropna()
            case Timeframe.HOURS_4:
                if self.symbol_type == SymbolType.STOCK:
                    df.index = df.index.tz_convert(MARKET_TIMEZONE)
                    df = df.resample('4h', offset='30min').agg(self.AGG_DEFAULTS).dropna()
                    df.index = df.index.tz_convert(pytz.UTC)
                else:
                    df = df.resample('4h').agg(self.AGG_DEFAULTS).dropna()
            case Timeframe.DAYS_1:
                pass
            case Timeframe.WEEKS_1:
                df_weekly = df.copy()
                df_weekly.index = df_weekly.index - pd.to_timedelta(df_weekly.index.dayofweek, unit='D')
                df = df_weekly.resample('W-MON').agg(self.AGG_DEFAULTS).dropna()
            case _:
                df = df.resample(self.RESAMPLE_PERIODS[timeframe]).agg(self.AGG_DEFAULTS).dropna()

        return metrics.stratify_df(df)

    # def historical_from_db(self, tf: Timeframe):
    #     model = self.tf_pricerec_model_map[tf]
    #     exchange_q = Q(exchange=self.exchange) if self.symbol_type == SymbolType.CRYPTO else Q()
    #     dt = (timezone.now() - self.HISTORICAL_TIMEDELTAS[tf]).astimezone()
    #     match tf:
    #         case Timeframe.MINUTES_1:
    #             return self.one
    #         case Timeframe.MINUTES_60:
    #             if self.symbol_type == SymbolType.STOCK:
    #                 kwargs = {'offset': '30min'}
    #                 df = self.thirty.resample('60min', **kwargs).agg(self.AGG_DEFAULTS).dropna()
    #             else:
    #                 df = model.df.filter(exchange_q, symbol=self.symbol, bucket__gte=dt).as_df()
    #         case Timeframe.HOURS_4:
    #             if self.symbol_type == SymbolType.STOCK:
    #                 df = self.thirty.copy()
    #                 df.index = df.index.tz_convert(MARKET_TIMEZONE)
    #                 df = df.resample('4h', offset='9h30min').agg(self.AGG_DEFAULTS).dropna()
    #                 df.index = df.index.tz_convert(pytz.UTC)
    #             else:
    #                 df = model.df.filter(exchange_q, symbol=self.symbol, bucket__gte=dt).as_df()
    #         case _:
    #             df = model.df.filter(exchange_q, symbol=self.symbol, bucket__gte=dt).as_df()
    #     return metrics.stratify_df(df)

    def historical_from_redis(self, tf: Timeframe):
        if tf not in self.scan_timeframes:
            raise ValueError(f'invalid timeframe. valid timeframes: {self.scan_timeframes}')

        df_bytes = r.get(f'df:{self.symbol_type}:{self.symbol}:{tf}')
        df = pickle.loads(df_bytes) if df_bytes else pd.DataFrame()
        df.rename(columns={
            'o': 'open',
            'h': 'high',
            'l': 'low',
            'c': 'close',
            'v': 'volume',
        }, inplace=True)

        return metrics.stratify_df(df)

    @cached_property
    def one(self):
        model = StockPriceRec if self.symbol_type == SymbolType.STOCK else CryptoPriceRec
        df = model.df.filter(symbol=self.symbol).order_by('-time')[:50000].to_timeseries(index='time')
        df.sort_index(inplace=True)
        return df

    @cached_property
    def fifteen(self):
        return self.historical_from_redis(Timeframe.MINUTES_15)

    @cached_property
    def thirty(self):
        return self.historical_from_redis(Timeframe.MINUTES_30)

    @cached_property
    def hourly(self):
        return self.historical_from_redis(Timeframe.MINUTES_60)

    @cached_property
    def four_hour(self):
        return self.historical_from_redis(Timeframe.HOURS_4)

    @cached_property
    def six_hour(self):
        return self.historical_from_redis(Timeframe.HOURS_6)

    @cached_property
    def twelve_hour(self):
        return self.historical_from_redis(Timeframe.HOURS_12)

    @cached_property
    def daily(self):
        return self.historical_from_redis(Timeframe.DAYS_1)

    @cached_property
    def daily_db(self):
        model = StockPriceRecViewD if self.symbol_type == SymbolType.STOCK else CryptoPriceRecViewD
        return model.df.filter(symbol=self.symbol).as_df()

    @cached_property
    def weekly(self):
        return self.historical_from_redis(Timeframe.WEEKS_1)

    @cached_property
    def monthly(self):
        return self.historical_from_redis(Timeframe.MONTHS_1)

    @cached_property
    def quarterly(self):
        return self.historical_from_redis(Timeframe.QUARTERS_1)

    @cached_property
    def yearly(self):
        return self.historical_from_redis(Timeframe.YEARS_1)

    @property
    def todays_volume(self) -> Union[int, float]:
        try:
            ohlc = r.json().get(f'barHistory:{self.symbol_type}:{self.symbol}').get('D')[-1]
            volume = ohlc['v']
        except (TypeError, AttributeError, IndexError):
            volume = 0
        return volume

    def opening_prices(self, timeframes: list = None) -> dict:
        timeframes = timeframes or self.tfc_timeframes
        tf_bars = r.json().get(f'barHistory:{self.symbol_type}:{self.symbol}')
        return {tf: bars[-1]['o'] for tf, bars in tf_bars.items() if tf in timeframes}

    def tfc_state(self, timeframes: list = None) -> dict:
        timeframes = timeframes or self.tfc_timeframes
        opening_prices = self.opening_prices(timeframes)
        tfc_table = {}
        for tf in timeframes:
            try:
                open_price = opening_prices[tf]
            except KeyError:
                continue
            distance_usd = self.price - open_price
            distance_ratio = round(distance_usd / open_price, 5)
            distance_percent = round(distance_ratio, 5)
            color = 'green' if distance_ratio > 0 else 'red' if distance_ratio < 0 else 'white'
            tfc_table[tf] = TFCState(
                open=open_price,
                distance_usd=distance_usd,
                distance_ratio=distance_ratio,
                distance_percent=distance_percent,
                color=color,
            )
        return tfc_table

    @property
    def tfc(self):
        return {k: asdict(v) for k, v in self.tfc_state(['D', 'W', 'M', 'Q', 'Y']).items()}

    def has_tfc(self, timeframes: list = None) -> tuple:
        if not timeframes:
            timeframes = self.scan_timeframes
        opening_prices = self.opening_prices(timeframes)
        if all([self.price > o for o in opening_prices.values()]):
            tfc_direction = 1
        elif all([self.price < o for o in opening_prices.values()]):
            tfc_direction = -1
        else:
            tfc_direction = 0
        return tfc_direction != 0, tfc_direction

    def tfc_min(self, timeframes: list):
        return min([x.distance_percent for x in self.tfc_state(timeframes).values()])

    def tfc_max(self, timeframes: list):
        return max([x.distance_percent for x in self.tfc_state(timeframes).values()])

    @staticmethod
    def calculate_rr(trigger: Decimal, target: Decimal, stop: Decimal) -> Decimal:
        rr = 0.0
        if target > trigger and trigger - stop > 0:
            rr = (target - trigger) / (trigger - stop)
        elif target < trigger and stop - trigger > 0:
            rr = (trigger - target) / (stop - trigger)
        return round(rr, 2)

    def get_setup_expiration(self, tf: Timeframe, timestamp: pd.Timestamp) -> datetime:
        """
        Get the expiration datetime for a setup.

        We don't know the current candle datetime. The candle we know about is at least one timeframe behind, so
        we multiply CANDLE_EXPIRE_DELTAS by 2. This only works for crypto and stock timeframes >= weekly. That's the
        reason for this line, twice:

            expires = expires + self.CANDLE_EXPIRE_DELTAS[tf]

        """
        if self.symbol_type == SymbolType.CRYPTO:
            return timestamp + self.CANDLE_EXPIRE_DELTAS[tf] * 2

        expires = timestamp + self.CANDLE_EXPIRE_DELTAS[tf]

        if tf in TIMEFRAMES_INTRADAY and self.exchange_calendar.is_closed(dt=expires):
            schedule = self.exchange_calendar.schedule(Timeframe.DAYS_1)
            mask = schedule['market_open'] > expires
            schedule = schedule[mask].head(1)
            expires = schedule['market_open'].iloc[0] + self.CANDLE_EXPIRE_DELTAS[tf]

        elif tf in TIMEFRAMES_INTRADAY:
            expires = expires + self.CANDLE_EXPIRE_DELTAS[tf]

        elif tf == Timeframe.DAYS_1:
            schedule = self.exchange_calendar.schedule(tf)
            mask = schedule['market_open'] >= expires
            schedule = schedule[mask].head(1)
            expires = schedule['market_close'].iloc[0]

        elif tf in (Timeframe.WEEKS_1, Timeframe.MONTHS_1, Timeframe.QUARTERS_1, Timeframe.YEARS_1):
            expires = expires + self.CANDLE_EXPIRE_DELTAS[tf]
            schedule = self.exchange_calendar.schedule(tf)
            mask = schedule['market_open'] < expires
            schedule = schedule[mask].tail(1)
            expires = schedule['market_close'].iloc[0]

        return expires

    def is_df_open(self, df: pd.DataFrame, tf: Timeframe) -> bool:
        if self.symbol_type == SymbolType.CRYPTO:
            timestamp = df.iloc[-1].name
            if timezone.now() < timestamp + self.CANDLE_EXPIRE_DELTAS[tf]:
                return True

        if tf in {Timeframe.WEEKS_1, Timeframe.MONTHS_1, Timeframe.QUARTERS_1, Timeframe.YEARS_1}:
            schedule = self.exchange_calendar.schedule(tf)
            current_datetime = timezone.now()
            filtered_schedule = schedule[(schedule['market_open'] <= current_datetime) & (
                    schedule['market_close'] >= current_datetime)]
            if not filtered_schedule.empty:
                return True

        elif self.symbol_type == SymbolType.STOCK and self.exchange_calendar.is_open():
            if not (tf in TIMEFRAMES_INTRADAY and timezone.now() > df.iloc[-1].name +
                    self.CANDLE_EXPIRE_DELTAS[tf]):
                return True

        return False

    # @func_timer
    def scan_strat_setups(self, timeframes: set = None, rr_min: float = 0.0) -> dict:
        timeframes = timeframes or self.scan_timeframes
        setups_found = {}

        for tf in timeframes:
            df = getattr(self, self.TF_MAP[tf])
            if df is None or isinstance(df, pd.DataFrame) and df.empty:
                continue

            tf = Timeframe(tf)
            if self.is_df_open(df, tf):
                df = df.head(-1)

            try:
                candle_pair = CandlePair(
                    trigger_candle=df.iloc[-1],
                    target_candle=df.iloc[-2],
                )
            except IndexError:
                logger.info(f'[{self.symbol}] [{tf}] not enough candles')
                continue

            setups = []

            priority = candle_pair.prioritize_setup()
            if not priority:
                continue

            # if candle_pair.trigger_candle.strat_id == '3':
            #     if direction := candle_pair.trigger_candle_direction:
            #         builder = SetupBuilder(self, tf, direction, df)
            #         setup = (
            #             builder
            #             .with_candle_pair(candle_pair)
            #             .with_priority(priority)
            #             .with_rr(rr_min)
            #             .with_pmg()
            #             .build()
            #         )
            #         if setup:
            #             setups.append(setup)
            # else:
            if candle_pair.trigger_candle.strat_id == '3':
                continue

            for direction in self.TRADE_DIRECTIONS[candle_pair.trigger_candle.strat_id]:
                builder = SetupBuilder(self, tf, direction, df)
                setup = (
                    builder
                    .with_candle_pair(candle_pair)
                    .with_priority(priority)
                    .with_rr(rr_min)
                    .with_pmg()
                    .build()
                )
                if setup:
                    setups.append(setup)

            if setups:
                setups_found[tf] = setups

        return setups_found


class ProviderMeta(models.Model):
    name = models.CharField(max_length=200)
    last_updated = models.DateTimeField(auto_now=True)
    meta = models.JSONField("Meta", null=True, blank=True)
    symbolrec = models.ForeignKey(SymbolRec, on_delete=models.CASCADE, related_name='provider_metas')

    def __str__(self):
        return f'{self.symbolrec.symbol} - {self.name} - {self.last_updated}'


@dataclass
class TFCState:
    # tf: str
    open: float
    distance_usd: float
    distance_ratio: float
    distance_percent: float
    color: str


class Direction(models.IntegerChoices):
    BEAR = -1, "BEAR"
    BULL = 1, "BULL"


class SetupBuilder:
    def __init__(
        self,
        symbolrec: SymbolRec,
        tf: Timeframe,
        direction: int,
        df: pd.DataFrame,
    ):
        self.symbolrec = symbolrec
        self.df = df
        timestamp = df.iloc[-1].name
        expires = symbolrec.get_setup_expiration(tf, timestamp)
        self.setup = Setup(symbol_rec=symbolrec, tf=tf, direction=direction, timestamp=timestamp, expires=expires)

        self.candle_pair: Optional[CandlePair] = None
        self.high_or_low = 'high' if direction == 1 else 'low'
        self.is_valid = True

    def find_targets(self, target):
        direction = self.setup.direction
        column = self.high_or_low

        condition = (self.df[column] >= target) if direction == 1 else (self.df[column] <= target)
        filtered = self.df.loc[condition, column].sort_index(ascending=False)

        potential_targets = filtered[abs((filtered - self.setup.trigger) / self.setup.trigger) >= 0.001]

        targets = []
        for value in potential_targets:
            if targets:
                if (direction == 1 and value <= targets[-1]) or (direction == -1 and value >= targets[-1]):
                    continue
            targets.append(value)

            if len(targets) == 5:
                break

        return targets

    def with_candle_pair(self, candle_pair):
        self.candle_pair = candle_pair
        self.setup.candle_tag = candle_pair.candle_tag

        trigger_candle = candle_pair.trigger_candle.to_dict()
        target_candle = candle_pair.target_candle.to_dict()

        # TODO: temp bug fix for nan in dataframe / series
        for k, v in trigger_candle.items():
            if pd.isnull(v):
                trigger_candle[k] = None

        for k, v in target_candle.items():
            if pd.isnull(v):
                target_candle[k] = None

        self.setup.trigger_candle = trigger_candle
        self.setup.target_candle = target_candle
        # self.setup.trigger_candle = candle_pair.trigger_candle.to_dict()
        # self.setup.target_candle = candle_pair.target_candle.to_dict()
        self.setup.trigger = getattr(candle_pair.trigger_candle, self.high_or_low)
        self.setup.stop = candle_pair.stop
        self.setup.pattern = candle_pair.strat_pattern

        target = None
        if self.setup.pattern in [['1', '2U'], ['1', '1'], ['1', '2D']]:
            try:
                target = getattr(self.df.iloc[-3], self.high_or_low)
            except IndexError:
                pass
        # elif self.setup.pattern[1] == '3':
        #     try:
        #         target = getattr(candle_pair.trigger_candle, self.high_or_low)
        #     except AttributeError:
        #         pass
        else:
            target = getattr(candle_pair.target_candle, self.high_or_low)

        # print(self.setup.symbol_rec.symbol)
        # print(self.setup.tf)
        # print(target)
        if target is None:
            self.setup.targets = []
        else:
            self.setup.targets = self.find_targets(target)

        return self

    def with_priority(self, priority):
        self.setup.priority = priority
        return self

    def with_rr(self, rr_min: float = 0.0):
        if not self.setup.targets:
            self.setup.rr = 0.0
            return self

        trigger = Decimal(str(self.setup.trigger))
        target = Decimal(str(self.setup.targets[0]))
        stop = self.candle_pair.stop

        self.setup.rr = self.symbolrec.calculate_rr(trigger, target, stop)
        if self.setup.rr < rr_min:
            self.is_valid = False
        return self

    def with_pmg(self):
        _, self.setup.pmg = metrics.is_pmg(self.df, self.setup.direction, threshold=0)
        return self

    def build(self):
        required_attrs = {
            # 'pmg': self.setup.pmg,
            # 'priority': self.setup.priority,
        }

        for attr, value in required_attrs.items():
            if value is None:
                raise ValueError(f"The required attribute '{attr}' is not set.")

        if self.is_valid:
            return self.setup


class NegatedReason(models.Model):
    RR_MINIMUM = 1
    TFC_CONFLICT = 2
    MAG_THRESHOLD = 3
    GAPPING = 4
    POTENTIAL_OUTSIDE = 5

    REASONS = [
        (RR_MINIMUM, "RR Minimum"),
        (TFC_CONFLICT, "TFC Conflict"),
        (MAG_THRESHOLD, "Magnitude Threshold"),
        (GAPPING, "Gapping"),
        (POTENTIAL_OUTSIDE, "Potential Outside Bar"),
    ]

    reason = models.IntegerField(choices=REASONS)


class Setup(DirtyFieldsMixin, models.Model):
    def __str__(self):
        pattern = '-'.join(self.pattern)
        direction = Direction(self.direction).label
        trigger = self.trigger
        target = self.target
        expires = self.expires.strftime('%Y-%m-%d %H:%M')
        return f"[{self.tf}] {pattern} {direction}: {trigger} | {target} | {expires}"

    symbol_rec = models.ForeignKey(SymbolRec, on_delete=models.CASCADE, verbose_name="Symbol Record")
    target_candle = models.JSONField("Target Candle")
    trigger_candle = models.JSONField("Trigger Candle")
    candle_tag = models.CharField("Candle Tag", max_length=16, null=True, blank=True)
    timestamp = models.DateTimeField("Timestamp", db_index=True)
    expires = models.DateTimeField("Expires At")
    tf = models.CharField("Timeframe", max_length=3, choices=Timeframe.choices)
    pattern = ArrayField(models.CharField(max_length=3), size=2, verbose_name="Pattern")
    trigger = models.FloatField("Trigger")
    targets = ArrayField(models.FloatField(), verbose_name="Targets")
    stop = models.FloatField("Stop")
    direction = models.SmallIntegerField("Direction", choices=Direction.choices)
    rr = models.FloatField("Risk:Return")
    serialized_df = models.BinaryField("Dataframe", null=True, blank=True)
    priority = models.PositiveSmallIntegerField("Priority", default=0)
    pmg = models.IntegerField("PMG", default=0)
    has_triggered = models.BooleanField("Triggered", default=False)
    trigger_count = models.PositiveBigIntegerField("Trigger Count", default=0)
    initial_trigger = models.DateTimeField("Initial Trigger", blank=True, null=True, default=None)
    last_triggered = models.DateTimeField("Last Triggered", blank=True, null=True, default=None)
    in_force = models.BooleanField("In Force", default=False)
    in_force_alerted = models.BooleanField("In Force Alerted?", default=False)
    in_force_last_alerted = models.DateTimeField(
        "In Force Last Alerted", blank=True, null=True, default=None
    )
    hit_magnitude = models.BooleanField("Hit Magnitude", default=False)
    magnitude_alerted = models.BooleanField("Magnitude Alerted", default=False)
    magnitude_last_alerted = models.DateTimeField(
        "Magnitude Last Alerted", blank=True, null=True, default=None
    )
    potential_outside = models.BooleanField("Potential Outside Bar", default=False)
    discord_alerted = models.BooleanField("Discord Alerted", default=False)
    gapped = models.BooleanField("Gapped?", default=False)
    negated = models.BooleanField("Negated", default=False)
    negated_reasons = models.ManyToManyField(NegatedReason, blank=True)
    # objects = DataFrameManager()

    class Meta:
        constraints = [
            models.UniqueConstraint(
                fields=["symbol_rec", "timestamp", "tf", "pattern", "direction"],
                # condition=Q(is_expired=False),
                name="td__su__sr_pt_dr__abv_puix",
            )
        ]
        indexes = [
            models.Index(
                fields=["expires"],
                # condition=Q(is_expired=False),
                name="td__su__ex__abv_puix",
            ),
        ]
        ordering = ["-timestamp"]
        get_latest_by = "timestamp"
        verbose_name = "Setup"
        verbose_name_plural = "Setups"

    def near_trigger(self, last_price: float, threshold: float = 0.0) -> bool:
        trigger = Decimal(str(self.trigger))
        last_price = Decimal(str(last_price))
        percentage_difference = abs((last_price - trigger) / trigger) * 100
        return percentage_difference <= threshold

    def is_gapping(self, last_price: float, threshold: float = 0.0) -> bool:
        trigger = Decimal(str(self.trigger))
        last_price = Decimal(str(last_price))
        if self.direction == 1:
            percentage_change = ((last_price - trigger) / trigger) * 100
            return percentage_change > threshold
        elif self.direction == -1:
            percentage_change = ((trigger - last_price) / trigger) * 100
            return percentage_change > threshold

    @property
    def magnitude_percent(self) -> Decimal:
        trigger = Decimal(str(self.trigger))
        target = Decimal(str(self.targets[0]))
        return round(abs(trigger - target) / trigger * 100, 2)

    @property
    def magnitude_dollars(self) -> Decimal:
        trigger = Decimal(str(self.trigger))
        target = Decimal(str(self.target))
        return abs(trigger - target)

    @property
    def mag_threshold(self):
        if self.potential_outside:
            return 0.0
        elif self.tf in ['15', '30', '60']:
            return 0.10
        return 1.0

    @property
    def below_mag_threshold(self) -> bool:
        return self.magnitude_percent < self.mag_threshold

    @property
    def bull_or_bear(self):
        return Direction(self.direction).label

    @property
    def target(self):
        try:
            return self.targets[0]
        except IndexError:
            pass

    @property
    def is_pmg(self):
        return self.pmg >= 5


class DominoGroup(models.Model):
    symbol_rec = models.ForeignKey(SymbolRec, on_delete=models.CASCADE)
    direction = models.IntegerField()
    created_at = models.DateTimeField(auto_now_add=True)
    unique_uuid = models.UUIDField(default=uuid.uuid4, editable=False, unique=True)
    unique_hash = models.CharField(max_length=64, unique=True)
    expires = models.DateTimeField()


class DominoGroupMember(models.Model):
    setup = models.ForeignKey(Setup, on_delete=models.CASCADE)
    domino_group = models.ForeignKey(DominoGroup, on_delete=models.CASCADE)
