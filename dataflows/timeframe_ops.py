from datetime import datetime, timedelta, timezone
import math

import pytz


def floor_datetime_fixed(dt: datetime, delta: timedelta) -> datetime:
    """
    Floor the datetime for intervals with a fixed duration like minutes, hours, days, or weeks.
    This function uses a consistent timedelta, suitable for intervals of regular length.
    If datetime is timezone-aware, convert to UTC before flooring.
    """
    min_tz = datetime.min.replace(tzinfo=timezone.utc)
    return min_tz + math.floor((dt - min_tz) / delta) * delta
    # return datetime.min + math.floor((dt - datetime.min) / delta) * delta


def floor_datetime_variable(dt: datetime, interval: str) -> datetime:
    """
    Floor the datetime for intervals with variable durations, such as months (M), quarters (Q), and years (Y).
    These intervals can't be represented as a fixed duration due to varying lengths (like number of days per month,
    leap years).
    """
    if interval == 'M':
        return dt.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    elif interval == 'Q':
        quarter_month = 3 * ((dt.month - 1) // 3) + 1
        return dt.replace(month=quarter_month, day=1, hour=0, minute=0, second=0, microsecond=0)
    elif interval == 'Y':
        return dt.replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0)
    else:
        return dt


def _floor_to_start_of_period(dt, tz, period='day'):
    """
    Floor the datetime to the start of the day or week in a given timezone.
    """
    dt_tz = dt.astimezone(tz)
    if period == 'week':
        dt_tz -= timedelta(days=dt_tz.weekday())
    return tz.localize(datetime(dt_tz.year, dt_tz.month, dt_tz.day))


def floor_datetime_mixed(dt, delta, offset=timedelta(0)):
    """
    Floor the datetime for intervals with a fixed duration, considering an offset.
    For daily or weekly intervals, floor to UTC time, otherwise floor to EST.
    """
    market_tz = pytz.timezone("America/New_York")
    utc_tz = pytz.utc

    period = 'day' if delta < timedelta(days=7) else 'week'
    start_of_period = _floor_to_start_of_period(dt, utc_tz, period) if delta >= timedelta(days=1) else _floor_to_start_of_period(dt, market_tz)

    start_of_period_with_offset = start_of_period + offset
    intervals_since_start = (dt.astimezone(utc_tz) - start_of_period_with_offset) // delta
    floored_time = start_of_period_with_offset + intervals_since_start * delta

    return floored_time.astimezone(dt.tzinfo)


def make_stock_time_buckets(dt: datetime):
    """
    Create time buckets for datetime downsample.
    """
    return {
        '1': floor_datetime_mixed(dt, delta=timedelta(minutes=1)),
        '15': floor_datetime_mixed(dt, delta=timedelta(minutes=15)),
        '30': floor_datetime_mixed(dt, delta=timedelta(minutes=30)),
        '60': floor_datetime_mixed(dt, delta=timedelta(minutes=60), offset=timedelta(minutes=30)),
        '4H': floor_datetime_mixed(dt, delta=timedelta(hours=4), offset=timedelta(hours=9, minutes=30)),
        'D': floor_datetime_mixed(dt, delta=timedelta(days=1)),
        'W': floor_datetime_mixed(dt, delta=timedelta(weeks=1)),
        'M': floor_datetime_variable(dt, interval='M'),
        'Q': floor_datetime_variable(dt, interval='Q'),
        'Y': floor_datetime_variable(dt, interval='Y'),
    }


def make_crypto_time_buckets(dt: datetime) -> dict:
    return {
        '1': floor_datetime_fixed(dt, delta=timedelta(minutes=1)),
        '15': floor_datetime_fixed(dt, delta=timedelta(minutes=15)),
        '30': floor_datetime_fixed(dt, delta=timedelta(minutes=30)),
        '60': floor_datetime_fixed(dt, delta=timedelta(minutes=60)),
        '4H': floor_datetime_fixed(dt, delta=timedelta(hours=4)),
        '6H': floor_datetime_fixed(dt, delta=timedelta(hours=6)),
        '12H': floor_datetime_fixed(dt, delta=timedelta(hours=12)),
        'D': floor_datetime_fixed(dt, delta=timedelta(days=1)),
        'W': floor_datetime_fixed(dt, delta=timedelta(weeks=1)),
        'M': floor_datetime_variable(dt, interval='M'),
        'Q': floor_datetime_variable(dt, interval='Q'),
        'Y': floor_datetime_variable(dt, interval='Y'),
    }
