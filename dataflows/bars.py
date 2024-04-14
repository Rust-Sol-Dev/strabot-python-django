from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
import redis

import msgspec


@dataclass
class Bar:
    ts: float
    o: float
    h: float
    l: float
    c: float
    v: float
    sid: str | None = None
    # vwap: float | None = None

    @property
    def green(self):
        return self.c > self.o

    @property
    def red(self):
        return self.c < self.o

    def color(self):
        return 'green' if self.green else 'red' if self.red else 'white'

    @property
    def as_dict(self):
        return self.__dict__

    @property
    def as_tuple(self):
        return self.sid, bar_shape(self), self.green, self.red


class BarSeries:
    MAXLEN = 5
    _bars: dict[float, Bar]

    def __init__(self, symbol: str, tf: str, bars: dict[float, Bar] = None):
        self.symbol = symbol
        self.tf = tf
        self._bars = bars or {}

    def __str__(self):
        return f'{self.symbol} {self.tf} - {len(self._bars)} bars'

    @property
    def bars(self) -> dict[float, Bar]:
        return self._bars

    def add_bar(self, bar):
        if len(self._bars) >= self.MAXLEN:
            self._bars.pop(min(self._bars.keys()))
        self._bars[bar.ts] = bar

    def get_newest(self):
        return self._bars[max(self._bars.keys())] if self._bars else None

    def get_previous(self):
        sorted_keys = sorted(self._bars.keys())
        return self._bars[sorted_keys[-2]] if len(sorted_keys) > 1 else None

    def get_by_index(self, index):
        sorted_keys = sorted(self._bars.keys())
        return self._bars[sorted_keys[index]] if len(sorted_keys) > index else None

    def replace_newest(self, bar):
        if self._bars:
            self._bars.pop(max(self._bars.keys()))
        self._bars[bar.ts] = bar

    def merge_bar_from_timescale(self, new_bar):
        existing_bar = self.get_newest()
        if existing_bar is None:
            self.add_bar(new_bar)
            return

        if existing_bar.ts == new_bar.ts:
            existing_bar.o = new_bar.o
            existing_bar.h = max(existing_bar.h, new_bar.h, float('-inf'))
            existing_bar.l = min(existing_bar.l, new_bar.l, float('inf'))
            existing_bar.c = new_bar.c
        else:
            self.add_bar(new_bar)

    def strat_candles(self):
        if len(self._bars) < 3:
            return None, None
        trigger_bar = self.get_previous()
        sorted_keys = sorted(self._bars.keys())
        target_bar = self._bars[sorted_keys[-3]] if len(sorted_keys) > 2 else None
        return target_bar, trigger_bar

    @property
    def outside_trigger(self):
        if len(self._bars) < 2:
            return None
        previous_bar = self.get_previous()
        high = Decimal(str(previous_bar.h))
        low = Decimal(str(previous_bar.l))
        return (high + low) / 2

    def as_dict(self):
        return [bar.__dict__ for bar in self.bars.values()]


def bar_series_from_cache(r: redis.client, key_prefix: str, symbol: str, tf: str):
    bar_series = BarSeries(symbol, tf)
    try:
        bars = r.json().get(f'{key_prefix}{symbol}', tf)
        for bar in bars:
            bar_series.add_bar(Bar(**bar))
    except redis.exceptions.ResponseError:
        pass
    return bar_series


# def bar_series_from_db(symbol, tf):
#     symbolrec = SymbolRec.objects.get(symbol=symbol)
#
#     df = getattr(symbolrec, symbolrec.TF_MAP[tf])
#     df = df.tail(5).reset_index()
#     df['time'] = (df['time'].astype('int64') // 1e9)
#     df = df[['time', 'open', 'high', 'low', 'close', 'volume', 'strat_id']]
#     df.rename(columns={
#         'time': 'ts',
#         'open': 'o',
#         'high': 'h',
#         'low': 'l',
#         'close': 'c',
#         'volume': 'v',
#         'strat_id': 'sid',
#     }, inplace=True)
#     bars_dict = df.to_dict(orient='records')
#
#     bar_series = BarSeries(symbol, tf)
#     for bar_ in bars_dict:
#         bar_series.add_bar(Bar(**bar_))
#
#     return bar_series


def strat_id(previous_bar: Bar, newest_bar: Bar) -> str:
    if (newest_bar.h <= previous_bar.h) & (newest_bar.l >= previous_bar.l):
        sid = '1'
    elif (newest_bar.h > previous_bar.h) & (newest_bar.l < previous_bar.l):
        sid = '3'
    elif newest_bar.h > previous_bar.h:
        sid = '2U'
    elif newest_bar.l < previous_bar.l:
        sid = '2D'
    else:
        sid = None
    return sid


def bar_shape(bar: Bar) -> str | None:
    body_length = abs(bar.o - bar.c)
    upper_shadow = bar.h - max(bar.o, bar.c)
    lower_shadow = min(bar.o, bar.c) - bar.l
    total_range = bar.h - bar.l

    if total_range == 0:
        return None

    top_threshold = 0.3  # top 30% of the candle
    bottom_threshold = 0.3  # bottom 30% of the candle

    # calculate the positions of open and close relative to the total range
    open_position = (bar.o - bar.l) / total_range
    close_position = (bar.c - bar.l) / total_range

    hammer = (close_position >= (1 - top_threshold)) and (lower_shadow >= 1.5 * body_length)  # long lower shadow
    shooter = (close_position <= bottom_threshold) and (upper_shadow >= 1.5 * body_length)  # long upper shadow

    conditions = [hammer, shooter]
    choices = ['hammer', 'shooter']

    result = None
    for condition, choice in zip(conditions, choices):
        if condition:
            result = choice

    return result


def potential_outside_bar(previous_bar: Bar, current_bar: Bar) -> tuple[bool, int]:
    price = current_bar.c
    outside_trigger = (previous_bar.h + previous_bar.l) / 2
    upward = price >= outside_trigger and current_bar.l < previous_bar.l
    downward = price <= outside_trigger and current_bar.h > previous_bar.h

    direction = 1 if upward else -1 if downward else None
    return upward or downward, direction


# def calculate_vwap(prices_volumes):
#     prices, volumes = zip(*prices_volumes)
#     return sum(price * volume for price, volume in zip(prices, volumes)) / sum(volumes)


# def filter_std_dev(prices):
#     """
#     Return a list of prices that are within two standard deviations from the average.
#     Print a message if there's an outlier with its price.
#     """
#     if len(prices) < 100:
#         return prices
#     average = statistics.mean(prices)
#     stdev = statistics.stdev(prices)
#     filtered_prices = []
#     for price in prices:
#         if abs(price - average) <= 2 * stdev:
#             filtered_prices.append(price)
#         else:
#             print(sorted(prices))
#             print(f'Outlier detected: {price}')  # print a message if there's an outlier
#     return filtered_prices


def to_ohlc(symbol__metadata__prices_volumes):
    symbol, (metadata, prices_volumes) = symbol__metadata__prices_volumes
    prices, volumes = zip(*prices_volumes)

    bar = Bar(
        ts=metadata.open_time.timestamp(),
        o=prices[0],
        h=max(prices),
        l=min(prices),
        c=prices[-1],
        v=sum(volumes),
    )
    return symbol, bar


def advance_decline(symbol__bars_by_tf):
    symbol, bars_by_tf = symbol__bars_by_tf

    daily_open = bars_by_tf['D'][-1]['o']
    price = bars_by_tf['D'][-1]['c']

    if price > daily_open:
        print(f'{symbol} advancing')
        direction = 1
    elif price < daily_open:
        print(f'{symbol} declining')
        direction = -1
    else:
        print(f'{symbol} -----')
        direction = 0

    return symbol, {'direction': direction}


def parse_tfc(symbol__bars_by_tf):
    symbol, bars_by_tf = symbol__bars_by_tf
    opens = {}
    for tf, bars_ in bars_by_tf.items():
        open_ = bars_[-1]['o']
        close = bars_[-1]['c']
        if close > open_:
            direction = 1
        elif close < open_:
            direction = -1
        else:
            direction = 0
        opens[tf] = direction
    return symbol, opens


def clean(symbol__tf_bars):
    symbol, tf_bars = symbol__tf_bars
    timeframes = tf_bars.keys()
    sid_values = []

    for tf, _bars in tf_bars.items():
        try:
            sid_values.append(_bars[-1]['sid'])
            for bar in _bars[-2:]:
                print(tf, datetime.fromtimestamp(bar['ts'], tz=timezone.utc), bar)
        except (IndexError, KeyError):
            return None

    ts = datetime.now(tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
    sid_string = ' | '.join(f'{tf}: {sid}' for tf, sid in zip(timeframes, sid_values))
    return symbol, f'{ts} | {sid_string}'


def add_key_to_value(symbol__value):
    symbol, value = symbol__value
    return symbol, (symbol, value)


def to_bar_series_by_tf(symbol__tf_bars):
    symbol, tf_bars = symbol__tf_bars
    bars_by_tf = {}
    for tf, bars_ in tf_bars.items():
        bar_series = BarSeries(symbol, tf)
        for bar in bars_:
            bar_series.add_bar(Bar(**bar))
        bars_by_tf[tf] = bar_series
    return symbol, bars_by_tf


def to_bar_series_by_tf_windowed(symbol__tf_bars):
    symbol, (metadata, tf_bars) = symbol__tf_bars
    bars_by_tf = {}
    for tf, bars_ in tf_bars[0].items():
        bar_series = BarSeries(symbol, tf)
        for bar in bars_:
            bar_series.add_bar(Bar(**bar))
        bars_by_tf[tf] = bar_series
    return symbol, bars_by_tf


def opening_prices(tf_bar_series):
    opens = {}
    for tf, bar_series in tf_bar_series.items():
        current_bar = bar_series.get_newest()
        opens[tf] = Decimal(str(current_bar.o))
    return opens


@dataclass
class TFCState:
    open: Decimal
    distance_usd: Decimal
    distance_ratio: Decimal
    distance_percent: Decimal
    color: str


def tfc_state(opens, last_price):
    tfc_table = {}
    for tf, open_price in opens.items():
        distance_usd = last_price - open_price
        distance_ratio = round(distance_usd / open_price, 3)
        distance_percent = round(distance_ratio * 100, 3)
        color = 'green' if distance_ratio > 0 else 'red' if distance_ratio < 0 else 'white'
        tfc_table[tf] = TFCState(
            open=open_price,
            distance_usd=distance_usd,
            distance_ratio=distance_ratio,
            distance_percent=distance_percent,
            color=color,
        )
    return tfc_table


def calculate_basic_score(tfc_table):
    colors = [state.color for state in tfc_table.values()]
    green_count = colors.count('green')
    red_count = colors.count('red')
    total_count = len(colors)

    # Adjusting basic score to reflect proportion of green to red candles
    score_ratio = max(green_count, red_count) / total_count
    basic_score = round(score_ratio * 10)  # Scale score up to a max of 10

    return basic_score


def calculate_bonus_points(tfc_table):
    colors = [state.color for state in tfc_table.values()]
    bonus_points = 0
    consecutive_count = 1

    for i in range(1, len(colors)):
        if colors[i] == colors[i-1]:
            consecutive_count += 1
        else:
            if consecutive_count >= 3:
                bonus_points += 2 * consecutive_count  # Increase bonus for each consecutive timeframe
            consecutive_count = 1

    if consecutive_count >= 3:
        bonus_points += 2 * consecutive_count

    return bonus_points


# def calculate_tfc_direction(tfc_table):
#     """
#     timeframes from the day down determine shoter-term direction
#     timeframes from the week and up determine mid-term direction
#     timeframes from the quarter and up determine longer-term direction
#     """
#     short_term_tfs = ['60', '4H', '6H', '12H', 'D']
#     short_term_bull = all([state.distance_ratio > 0 for tf, state in tfc_table.items() if tf in short_term_tfs])
#     short_term_bear = all([state.distance_ratio < 0 for tf, state in tfc_table.items() if tf in short_term_tfs])
#
#     mid_term_tfs = ['W', 'M']
#     mid_term_bull = all([state.distance_ratio > 0 for tf, state in tfc_table.items() if tf in mid_term_tfs])
#     mid_term_bear = all([state.distance_ratio < 0 for tf, state in tfc_table.items() if tf in mid_term_tfs])
#
#     long_term_tfs = ['Q', 'Y']
#     long_term_bull = all([state.distance_ratio > 0 for tf, state in tfc_table.items() if tf in long_term_tfs])
#     long_term_bear = all([state.distance_ratio < 0 for tf, state in tfc_table.items() if tf in long_term_tfs])


def calculate_tfc_direction(tfc_table):
    timeframe_conditions = {}

    short_term_tfs = ['60', '4H', '6H', '12H']
    if all([state.distance_ratio > 0 for tf, state in tfc_table.items() if tf in short_term_tfs]):
        timeframe_conditions['S'] = 'BULL'
    elif all([state.distance_ratio < 0 for tf, state in tfc_table.items() if tf in short_term_tfs]):
        timeframe_conditions['S'] = 'BEAR'
    else:
        timeframe_conditions['S'] = 'MIXED'

    mid_term_tfs = ['D', 'W']
    if all([state.distance_ratio > 0 for tf, state in tfc_table.items() if tf in mid_term_tfs]):
        timeframe_conditions['M'] = 'BULL'
    elif all([state.distance_ratio < 0 for tf, state in tfc_table.items() if tf in mid_term_tfs]):
        timeframe_conditions['M'] = 'BEAR'
    else:
        timeframe_conditions['M'] = 'MIXED'

    long_term_tfs = ['M', 'Q', 'Y']
    if all([state.distance_ratio > 0 for tf, state in tfc_table.items() if tf in long_term_tfs]):
        timeframe_conditions['L'] = 'BULL'
    elif all([state.distance_ratio < 0 for tf, state in tfc_table.items() if tf in long_term_tfs]):
        timeframe_conditions['L'] = 'BEAR'
    else:
        timeframe_conditions['L'] = 'MIXED'

    return timeframe_conditions


# def calculate_tfc_score(tfc_table):
#     basic_score = calculate_basic_score(tfc_table)
#     bonus_points = calculate_bonus_points(tfc_table)
#     total_score = basic_score + bonus_points
#
#     return total_score

def calculate_tfc_score(tfc_table):
    # Define the groups
    short_term_tfs = ['60', '4H', '6H', '12H']
    mid_term_tfs = ['D', 'W']
    long_term_tfs = ['M', 'Q', 'Y']

    # Initialize group scores
    group_scores = {'S': 0, 'M': 0, 'L': 0}

    # Define scoring for each group based on trend alignment
    for group, tfs in [('S', short_term_tfs), ('M', mid_term_tfs), ('L', long_term_tfs)]:
        directions = [tfc_table[tf].distance_ratio > 0 for tf in tfs if tf in tfc_table]
        if all(directions):
            group_scores[group] = 10  # Fully bullish
        elif all(not direction for direction in directions):
            group_scores[group] = 10  # Fully bearish
        elif any(directions) and any(not direction for direction in directions):
            group_scores[group] = 5  # Mixed signals
        else:
            group_scores[group] = 0  # Neutral or no data

    # Calculate overall score by summing group scores
    overall_score = sum(group_scores.values())

    # Check alignment between groups for bonus points
    if group_scores['S'] == group_scores['M'] == group_scores['L']:
        overall_score += 5  # Bonus for full alignment

    return overall_score, group_scores


def timeframes_colored(tfc_table) -> str:
    excluded_timeframes = ['15', '30', '4H', '6H', '12H']
    output = []
    for tf, state in tfc_table.items():
        if tf in excluded_timeframes:
            continue

        if state.distance_ratio > 0:
            output.append(f'{tf}:green_circle:')
        elif state.distance_ratio < 0:
            output.append(f'{tf}:red_circle:')
        else:
            output.append(f'{tf}:white_circle:')
    return ' '.join(output)

#
# def check_ftfc(symbol__tf_bar_series):
#     symbol, tf_bar_series = symbol__tf_bar_series
#
#     opens = opening_prices(tf_bar_series)
#     price = Decimal(str(tf_bar_series['15'].get_newest().c))
#     if all([price > o for tf, o in opens.items() if tf in ['60', 'D', 'W', 'M', 'Q', 'Y']]):
#         return symbol, (tf_bar_series, 1)
#     elif all([price < o for tf, o in opens.items() if tf in ['60', 'D', 'W', 'M', 'Q', 'Y']]):
#         return symbol, (tf_bar_series, -1)
#     else:
#         return
#
#
# def filter_continuation(symbol__tf_bar_series__tf_setups):
#     symbol, (tf_bar_series, tf_setups) = symbol__tf_bar_series__tf_setups
#
#     for tf, setup in tf_setups.items():
#         if setup is None:
#             continue
#         current_bar = tf_bar_series[tf].get_newest()
#         continuation = setup.trigger_bar.sid == current_bar.sid or (setup.trigger_bar.sid == '3' and current_bar.sid != '1')
#         if continuation:
#             tf_setups[tf] = None
#
#     return symbol, (tf_bar_series, tf_setups)
#
#
# def filter_ftfc(symbol__tf_bar_series__tf_setups):
#     symbol, (tf_bar_series, tf_setups) = symbol__tf_bar_series__tf_setups
#
#     for tf, setup in tf_setups.items():
#         if setup is None:
#             continue
#
#         opens = opening_prices(tf_bar_series)
#         price = Decimal(str(tf_bar_series['15'].get_newest().c))
#         if all([price > o for tf, o in opens.items() if tf in ['60', 'D', 'W', 'M', 'Q', 'Y']]):
#             setup.direction = 1
#         elif all([price < o for tf, o in opens.items() if tf in ['60', 'D', 'W', 'M', 'Q', 'Y']]):
#             setup.direction = -1
#         else:
#             tf_setups[tf] = None
#
#     return symbol, (tf_bar_series, tf_setups)
#
#
# def check_potential_outside(symbol__tf_bar_series__tf_setups):
#     symbol, (tf_bar_series, tf_setups) = symbol__tf_bar_series__tf_setups
#
#     for tf, setup in tf_setups.items():
#         if setup is None:
#             continue
#         current_bar = tf_bar_series[tf].get_newest()
#         potential_outside, direction = potential_outside_bar(setup.trigger_bar, current_bar)
#         if potential_outside:
#             setup.potential_outside = True
#
#     return symbol, (tf_bar_series, tf_setups)