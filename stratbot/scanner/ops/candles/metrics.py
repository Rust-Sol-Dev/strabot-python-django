from datetime import datetime
from decimal import Decimal, ROUND_HALF_UP
from dataclasses import dataclass

import pandas as pd
import numpy as np
import pytz
#import talib


def filter_premarket(df: pd.DataFrame) -> pd.DataFrame:
    df.index = df.index.tz_convert('America/New_York')
    df = df.between_time('9:30', '15:59')
    df.index = df.index.tz_convert(pytz.UTC)
    return df


def pivots(df: pd.DataFrame) -> pd.DataFrame:
    """
    calculate pivot points.
    to merge results with the original dataframe, use:
        df.join(returned_df, how='inner')
    """
    high = df.iloc[-1]['high']
    low = df.iloc[-1]['low']
    close = df.iloc[-1]['close']
    pivot = (high + low + close) / 3
    r1 = 2 * pivot - low
    s1 = 2 * pivot - high
    r2 = pivot + (high - low)
    s2 = pivot - (high - low)
    r3 = pivot + 2 * (high - low)
    s3 = pivot - 2 * (high - low)
    return pd.DataFrame(
        {
            'pivot': pivot,
            'r1': r1,
            's1': s1,
            'r2': r2,
            's2': s2,
            'r3': r3,
            's3': s3,
        },
        index=[df.iloc[-1].name]
    )


def stratify_df(df: pd.DataFrame) -> pd.DataFrame:
    try:
        df = df.copy()
    except AttributeError as e:
        return pd.DataFrame()

    df = strat_identification(df)
    df = candle_shape(df)

    df['green'] = df['close'] > df['open']
    df['red'] = df['close'] < df['open']

    # df.dropna(inplace=True)
    # df = df.where(pd.notnull(df), None)
    return df


def atr_metrics(df: pd.DataFrame, period: int = 14) -> tuple[Decimal, float]:
    high = df['high']
    low = df['low']
    close = df['close']
    atr = talib.ATR(high, low, close, timeperiod=14)
    # quantize_decial = Decimal('0.001') if symbol_type == SymbolType.STOCK else Decimal('0.00000001')
    # amount = Decimal(str(atr.iloc[-1])).quantize(quantize_decial, rounding=ROUND_HALF_UP)
    amount = atr.iloc[-1]
    percentage = (atr.iloc[-1] / close.iloc[-1]) * 100
    return amount, round(percentage, 2)


# def calc_atr(df: pd.DataFrame, period: int = 14) -> pd.DataFrame:
#     df['prev_close'] = df['close'].shift(1)
#     df['hl'] = df['high'] - df['low']
#     df['hc'] = abs(df['high'] - df['prev_close'])
#     df['lc'] = abs(df['low'] - df['prev_close'])
#     df['tr'] = df[['hl', 'hc', 'lc']].max(axis=1)
#     df['atr'] = df['tr'].rolling(window=period).mean()
#     df.drop(columns=['prev_close', 'hl', 'hc', 'lc', 'tr'], inplace=True)
#     return df


# def candle_shape(df: pd.DataFrame) -> pd.DataFrame:
#     # Candle Shapes
#     df['body_length'] = abs(df['open'] - df['close'])
#     df['upper_shadow'] = df['high'] - df[['open', 'close']].max(axis=1)
#     df['lower_shadow'] = df[['open', 'close']].min(axis=1) - df['low']
#
#     hammer = (df['lower_shadow'] >= 1.5 * df['body_length']) & (df['upper_shadow'] <= df['body_length'])
#     shooter = (df['upper_shadow'] >= 1.5 * df['body_length']) & (df['lower_shadow'] <= df['body_length'])
#     # doji = df['body_length'] < 0.05 * (df['high'] - df['low'])
#
#     conditions = [hammer, shooter]
#     choices = ['hammer', 'shooter']
#     df['candle_shape'] = np.select(conditions, choices, default="")
#     df['candle_shape'] = df['candle_shape'].astype('category')
#     return df


# def candle_shape(df: pd.DataFrame) -> pd.DataFrame:
#     # Candle Shapes
#     df['body_length'] = abs(df['o'] - df['c'])
#     df['upper_shadow'] = df['h'] - df[['o', 'c']].max(axis=1)
#     df['lower_shadow'] = df[['o', 'c']].min(axis=1) - df['l']
#     df['total_range'] = df['h'] - df['l']
#     df['body_percentage'] = df['body_length'] / df['total_range']
#
#     # Define thresholds for adjusting conditions
#     upper_shadow_threshold = 0.15  # Allow upper shadow to be up to 15% of the total range
#     body_percentage_threshold = 0.1  # Adjust conditions if body is less than 10% of the total range
#
#     # Calculate the conditions for a hammer
#     is_hammer_lower = df['lower_shadow'] >= 1.5 * df['body_length']
#     is_hammer_upper = (df['upper_shadow'] <= df['body_length']) | (
#         (df['body_percentage'] < body_percentage_threshold) &
#         (df['upper_shadow'] <= upper_shadow_threshold * df['total_range'])
#     )
#     hammer = is_hammer_lower & is_hammer_upper
#
#     # Define condition for shooter - this remains unchanged, adjust if needed
#     shooter = (df['upper_shadow'] >= 1.5 * df['body_length']) & (df['lower_shadow'] <= df['body_length'])
#
#     # Determine the candle shape
#     conditions = [hammer, shooter]
#     choices = ['hammer', 'shooter']
#     df['candle_shape'] = np.select(conditions, choices, default="")
#
#     # Set the 'candle_shape' column as a category type if needed
#     df['candle_shape'] = df['candle_shape'].astype('category')
#
#     return df


def candle_shape(df: pd.DataFrame) -> pd.DataFrame:
    df['body_length'] = abs(df['open'] - df['close'])
    df['upper_shadow'] = df['high'] - df[['open', 'close']].max(axis=1)
    df['lower_shadow'] = df[['open', 'close']].min(axis=1) - df['low']
    df['total_range'] = df['high'] - df['low']

    top_threshold = 0.3  # top 30% of the candle
    bottom_threshold = 0.3  # bottom 30% of the candle

    # calculate the positions of open and close relative to the total range
    df['open_position'] = (df['open'] - df['low']) / df['total_range']
    df['close_position'] = (df['close'] - df['low']) / df['total_range']

    hammer = (
            (df['close_position'] >= (1 - top_threshold)) &  # close is in the top 30%
            (df['lower_shadow'] >= 1.5 * df['body_length'])  # long lower shadow
    )

    shooter = (
            (df['close_position'] <= bottom_threshold) &  # close is in the bottom 30%
            (df['upper_shadow'] >= 1.5 * df['body_length'])  # long upper shadow
    )

    # body_threshold = 0.03  # body must be at least 3% of the total range
    # doji = df['body_length'] <= (body_threshold * df['total_range'])

    conditions = [hammer, shooter]
    choices = ['hammer', 'shooter']
    df['candle_shape'] = np.select(conditions, choices, default="")
    df['candle_shape'] = df['candle_shape'].astype('category')

    return df


def strat_identification(df: pd.DataFrame) -> pd.DataFrame:
    df['prev_high'] = df['high'].shift(1)
    df['prev_low'] = df['low'].shift(1)

    inside = (df['high'] <= df['prev_high']) & (df['low'] >= df['prev_low'])
    outside = (df['high'] > df['prev_high']) & (df['low'] < df['prev_low'])
    two_up = df['high'] > df['prev_high']
    two_down = df['low'] < df['prev_low']

    conditions = [inside, outside, two_up, two_down]
    choices = ['1', '3', '2U', '2D']
    df['strat_id'] = np.select(conditions, choices, default=np.nan)
    df['strat_id'] = df['strat_id'].astype('category')
    return df


def id_gaps(df: pd.DataFrame) -> pd.DataFrame:
    """Identify the unfilled gaps"""
    gap_ups = df['low'] > df['high'].shift(1)
    gap_downs = df['high'] < df['low'].shift(1)

    # Determine if gaps are filled
    upward_gaps_filled = (df['low'].reindex(df.index[::-1]).rolling(window=len(df), min_periods=1).min()[::-1] <= df[
        'high'].shift(1)) & gap_downs
    downward_gaps_filled = (df['high'].reindex(df.index[::-1]).rolling(window=len(df), min_periods=1).max()[::-1] >= df[
        'low'].shift(1)) & gap_ups

    # Label the gaps
    df['gap'] = ''
    df.loc[downward_gaps_filled, 'gap'] = 'down_filled'
    df.loc[upward_gaps_filled, 'gap'] = 'up_filled'
    df.loc[gap_downs & ~downward_gaps_filled, 'gap'] = 'down_unfilled'
    df.loc[gap_ups & ~upward_gaps_filled, 'gap'] = 'up_unfilled'
    return df


def calc_rvol(df: pd.DataFrame, period: int = 14) -> pd.DataFrame:
    average_volume = df['volume'].rolling(window=period).mean()
    df['rvol'] = (df['volume'] / average_volume)
    df['rvol'] = df['rvol'].fillna(0)
    return df


def calc_vwap(df: pd.DataFrame) -> pd.DataFrame:
    df['vwap'] = (df['volume'] * (df['high'] + df['low'] + df['close']) / 3).cumsum() / df['volume'].cumsum()
    return df


def is_pmg(df: pd.DataFrame, direction: int, threshold: int = 5) -> tuple[bool, int]:
    if direction == 1:
        highs = list(df.high)
        highs.reverse()
        lower_highs = 0
        for i in range(1, len(highs)):
            if highs[i - 1] <= highs[i]:
                lower_highs += 1
            else:
                break
        if lower_highs >= threshold:
            return True, lower_highs
    elif direction == -1:
        lows = list(df.low)
        lows.reverse()
        higher_lows = 0
        for i in range(1, len(lows)):
            if lows[i - 1] >= lows[i]:
                higher_lows += 1
            else:
                break
        if higher_lows >= threshold:
            return True, higher_lows
    return False, 0


def within_percentage(price1, price2, percentage=3):
    """check if two triggers are within a percentage of each other"""
    if price1 == price2:
        return False
    decimal_percentage = percentage / 100
    return abs(price1 - price2) <= decimal_percentage * ((price1 + price2) / 2)
