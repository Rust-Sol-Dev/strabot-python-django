import logging
from datetime import datetime, timedelta, time
from functools import cached_property
from typing import Final

import pytz
from django.core.cache import caches
import pandas as pd
import pandas_market_calendars as mcal

from .timeframes import Timeframe


logger = logging.getLogger(__name__)


MARKET_TIMEZONE: Final = pytz.timezone("America/New_York")


class ExchangeCalendar:
    """
    Wrapper around pandas_market_calendars to provide caching and convenience methods.
    """
    SCHEDULE_AGGS = {
        'pre': 'first',
        'market_open': 'first',
        'market_close': 'last',
        'post': 'last'
    }

    SCHEDULE_TIMEFRAMES = {
        Timeframe.DAYS_1,
        Timeframe.WEEKS_1,
        Timeframe.MONTHS_1,
        Timeframe.QUARTERS_1,
        Timeframe.YEARS_1,
    }

    def __init__(self, exchange_name: str = 'NYSE', start_date: datetime = None, end_date: datetime = None):
        self.cache = caches['markets']
        self.exchange_name = exchange_name
        self.calendar = mcal.get_calendar(self.exchange_name)
        self.schedule_cache_key = f'{self.exchange_name}_SCHEDULE'
        self.start_date = start_date or datetime.now() - timedelta(days=14)
        self.end_date = end_date or self.start_date + timedelta(days=365)

    def __repr__(self):
        return f'<{self.__class__.__name__}: {self.exchange_name}>'

    def __str__(self):
        return self.exchange_name

    @staticmethod
    def get_week_start_datetime(dt: datetime = None) -> datetime:
        dt = dt or datetime.now()
        days_to_subtract = dt.weekday()
        week_start_date = dt - timedelta(days=days_to_subtract)
        return datetime(week_start_date.year, week_start_date.month, week_start_date.day)

    @staticmethod
    def get_quarter_start_datetime(dt: datetime = None) -> datetime:
        dt = dt or datetime.now()
        month = dt.month
        if month <= 3:
            start_month = 1
        elif month <= 6:
            start_month = 4
        elif month <= 9:
            start_month = 7
        else:
            start_month = 10
        return datetime(dt.year, start_month, 1)

    def _cache_schedule(self, df: pd.DataFrame, timeframe: str) -> None:
        assert isinstance(df, pd.DataFrame)
        assert timeframe in self.SCHEDULE_TIMEFRAMES
        self.cache.set(f'{self.schedule_cache_key}:{timeframe}', df, timeout=None)

    def _schedule_from_cache(self, timeframe: str) -> pd.DataFrame:
        assert timeframe in self.SCHEDULE_TIMEFRAMES
        return self.cache.get(f'{self.schedule_cache_key}:{timeframe}')

    def cache_schedules(self) -> None:
        for timeframe in self.SCHEDULE_TIMEFRAMES:
            schedule = self.schedule(timeframe, from_cache=False)
            self._cache_schedule(schedule, timeframe)

    def schedule(self, timeframe: str, from_cache: bool = True) -> pd.DataFrame:
        assert timeframe in self.SCHEDULE_TIMEFRAMES

        df = self._schedule_from_cache(timeframe)
        if df is None or from_cache is False or df.empty:
            logger.info(f'generating {timeframe} schedule for {self.exchange_name}')
            df = self.calendar.schedule(start_date=self.start_date, end_date=self.end_date,
                                        market_times=['pre', 'market_open', 'market_close', 'post'])
            match timeframe:
                case Timeframe.WEEKS_1:
                    df = df.resample('W', label='left').agg(self.SCHEDULE_AGGS)
                case Timeframe.MONTHS_1:
                    df = df.resample('MS').agg(self.SCHEDULE_AGGS)
                case Timeframe.QUARTERS_1:
                    df = df.resample('QS').agg(self.SCHEDULE_AGGS)
                case Timeframe.YEARS_1:
                    df = df.resample('YS').agg(self.SCHEDULE_AGGS)
            # TODO: this was breaking the cache when backfill_db ran and set a start_date to 10 years ago
            # self._cache_schedule(df, timeframe)
        return df

    @cached_property
    def daily_schedule(self) -> pd.DataFrame:
        return self.schedule(Timeframe.DAYS_1)

    @cached_property
    def weekly_schedule(self) -> pd.DataFrame:
        return self.schedule(Timeframe.WEEKS_1)

    @cached_property
    def monthly_schedule(self) -> pd.DataFrame:
        return self.schedule(Timeframe.MONTHS_1)

    @cached_property
    def quarterly_schedule(self) -> pd.DataFrame:
        return self.schedule(Timeframe.QUARTERS_1)

    @cached_property
    def yearly_schedule(self) -> pd.DataFrame:
        return self.schedule(Timeframe.QUARTERS_1)

    def is_open(self, dt: datetime = None, only_rth: bool = True) -> bool:
        """
        Check if a market is open at a given datetime.
        :param dt: datetime to check if market is open
        :param only_rth: only check regular trading hours
        :return: True if market is open, False otherwise
        """
        if not dt:
            dt = datetime.utcnow()
        ny_time = dt.astimezone(pytz.timezone('America/New_York')).time()
        try:
            is_weekend = dt.isoweekday() in [6, 7]
            not_market_hours = (
                time(0, 0) <= ny_time < time(4, 0)) or (
                time(20, 0) <= ny_time <= time(0, 0)
            )
            if is_weekend or not_market_hours:
                return False
            is_open = self.calendar.open_at_time(self.schedule(Timeframe.DAYS_1), dt, only_rth=only_rth)
        except (IndexError, ValueError):
            is_open = False
        return is_open

    def is_closed(self, dt: datetime = None, only_rth: bool = True) -> bool:
        return not self.is_open(dt=dt, only_rth=only_rth)

    def is_premarket_hours(self, dt: datetime = None) -> bool:
        if not dt:
            dt = datetime.utcnow()
        ny_time = dt.astimezone(pytz.timezone('America/New_York')).time()
        premarket_hours = time(4, 0) <= ny_time < time(9, 30)
        if premarket_hours:
            is_open = self.calendar.open_at_time(self.schedule(Timeframe.DAYS_1), dt, only_rth=False)
            return is_open
        return False

    @staticmethod
    def to_est(dt: datetime) -> datetime:
        return dt.astimezone(pytz.timezone('America/New_York'))
