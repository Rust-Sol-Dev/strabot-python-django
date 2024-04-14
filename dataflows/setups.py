import copy
from datetime import datetime, timezone
from decimal import Decimal

import msgspec

from dataflows.bars import Bar, bar_shape, BarSeries, TFCState


def flat_map_setups(symbol__setups):
    symbol, setups = symbol__setups

    results = []
    for setup in setups:
        results.append((symbol, setup))
    return results
#
#
# # def flat_map_setups(symbol__setups__bars):
# #     symbol, (setups, bars) = symbol__setups__bars
# #
# #     results = []
# #     for setup in setups:
# #         results.append((symbol, setup, bars))
# #     return results
#
#
# def flat_map_setups_testing(symbol__setups):
#     symbol, setups = symbol__setups
#
#     results = []
#     for setup in setups:
#         results.append((f'{symbol}_{setup.pk}', setup))
#     return results
#
#
# def flat_map_setups_single_tf(symbol__setups__bars):
#     symbol, (setups, bars) = symbol__setups__bars
#
#     results = []
#     for setup in setups:
#         if tf_bars := bars.get(setup.tf):
#             results.append((symbol, setup, tf_bars))
#     return results
#
#


def drop_unused_timeframes(symbol__setup__bars):
    symbol, setup, bars = symbol__setup__bars

    return symbol, setup, bars[setup.tf]


# def tfc_state(tfs_bars):
#     tfc_table = {}
#     for tf, bars in tfs_bars.items():
#         try:
#             open_price = bars[-1]['o']
#         except KeyError:
#             continue
#
#         distance_usd = bars[-1]['c'] - open_price
#         distance_ratio = round(distance_usd / open_price, 5)
#         distance_percent = round(distance_ratio, 5)
#         tfc_table[tf] = {
#             'open': open_price,
#             'distance_usd': distance_usd,
#             'distance_ratio': distance_ratio,
#             'distance_percent': distance_percent,
#         }
#     return tfc_table


# def is_against_tfc(symbol__setup__bars):
#     symbol, setup, bars = symbol__setup__bars
#
#     try:
#         daily_open = bars['D'][-1]['o']
#     except (KeyError, IndexError):
#         return None
#
#     if setup.tf in ["15", "30"]:
#         if (setup.direction == -1 and setup.trigger > daily_open) or (
#             setup.direction == 1 and setup.trigger < daily_open
#         ):
#             print(f"TFC mismatch (daily): {symbol}, {setup}")
#             setup.negated = True
#
#     return symbol, setup, bars


# def is_potential_outside_bar(symbol__tf_bar_series):
#     symbol, tf_bars = symbol__tf_bar_series
#
#     outside_bar_timeframes = set()
#     for tf, bar_series in tf_bars.items():
#         previous_bar = bar_series.get_previous()
#         current_bar = bar_series.get_newest()
#         close = current_bar.c
#         outside_trigger = (previous_bar.h + current_bar.l) / 2
#
#         if tf not in ["15", "30"]:
#             upward = close > outside_trigger and current_bar.l < previous_bar.l
#             downward = close < outside_trigger and current_bar.h > previous_bar.h
#             if upward or downward:
#                 print(f'potential outside bar: {symbol} {tf}')
#                 outside_bar_timeframes.add(tf)
#     return symbol, outside_bar_timeframes


def is_in_force(symbol__setup__bars):
    symbol, setup, bars = symbol__setup__bars

    close = bars[-1]['c']
    triggered_lower = close < setup.trigger and setup.direction == -1
    triggered_higher = close > setup.trigger and setup.direction == 1
    setup.in_force = triggered_lower or triggered_higher

    return symbol, setup


def hit_magnitude(symbol__setup__bars):
    symbol, setup, bars = symbol__setup__bars

    try:
        current_bar = bars[-1]
    except IndexError:
        return

    target = setup.targets[0]
    if (setup.direction == 1 and current_bar['h'] >= target) or (
        setup.direction == -1 and current_bar['l'] <= target
    ):
        setup.hit_magnitude = True
        return symbol, setup


# --------------------------------------------------------------------------------------------------

class SetupMsg(msgspec.Struct):
    symbol: str
    timestamp: datetime
    tf: str
    pattern: list[str]
    trigger_bar: Bar
    target_bar: Bar
    current_bar: Bar | None
    # tfc_table: dict[str, TFCState] = {}
    direction: int = 0
    initial_trigger: datetime | None = None
    trigger: Decimal | None = None
    trigger_count: int = 0
    target: Decimal | None = None
    # targets: list[Decimal] | None = None
    potential_outside: bool = False
    in_force: bool = False
    in_force_alerted: bool = False
    in_force_last_alerted: datetime | None = None
    hit_magnitude: bool = False
    magnitude_alerted: bool = False
    magnitude_last_alerted: datetime | None = None
    priority: int | None = None
    shape: str | None = None
    negated: bool = False
    negated_reasons: set[str] = set()
    notes: str = ''

    def __str__(self):
        return f'{self.timestamp} [{self.tf}] {self.pattern} [{self.shape}]'

    # @property
    # def trigger(self):
    #     if self.direction == 1:
    #         return self.bull_trigger
    #     elif self.direction == -1:
    #         return self.bear_trigger
    #
    @property
    def bull_trigger(self):
        return Decimal(str(self.trigger_bar.h))

    @property
    def bear_trigger(self):
        return Decimal(str(self.trigger_bar.l))

    @property
    def outside_trigger(self):
        return (self.bear_trigger + self.bull_trigger) / 2

    # @property
    # def target(self):
    #     if self.direction == 1:
    #         return self.bull_target
    #     elif self.direction == -1:
    #         return self.bear_target

    @property
    def bull_target(self):
        return Decimal(str(self.target_bar.h))

    @property
    def bear_target(self):
        return Decimal(str(self.target_bar.l))

    @property
    def magnitude_percent(self) -> Decimal | None:
        if self.target is None:
            return None
        trigger = Decimal(str(self.trigger))
        target = Decimal(str(self.target))
        return round(abs(trigger - target) / trigger * 100, 2)

    @property
    def magnitude_dollars(self) -> Decimal | None:
        if self.target is None:
            return None
        trigger = Decimal(str(self.trigger))
        target = Decimal(str(self.target))
        return abs(trigger - target)

    def check_potential_outside(self, current_bar: Bar):
        price = current_bar.c
        # outside_trigger = (self.trigger_bar.h + self.trigger_bar.l) / 2
        up_in_force = price >= self.outside_trigger and current_bar.l < self.trigger_bar.l
        down_in_force = price <= self.outside_trigger and current_bar.h > self.trigger_bar.h

        if up_in_force or down_in_force:
            self.direction = 1 if up_in_force else -1 if down_in_force else 0
            self.in_force = True
            # self.trigger = Decimal(str(outside_trigger))
            self.trigger = self.outside_trigger
            self.potential_outside = True
            self.pattern = [self.trigger_bar.sid, 'P3']

    def check_in_force(self, current_bar: Bar):
        in_force_bear = current_bar.c < self.trigger_bar.l
        in_force_bull = current_bar.c > self.trigger_bar.h
        self.in_force = in_force_bear or in_force_bull

        if in_force_bear:
            self.direction = -1
            self.trigger = self.bear_trigger
            # self.target = self.bear_target
        elif in_force_bull:
            self.direction = 1
            self.trigger = self.bull_trigger
            # self.target = self.bull_target

    @property
    def bull_or_bear(self) -> str:
        return 'BULL' if self.direction == 1 else 'BEAR' if self.direction == -1 else 'NEUTRAL'



def bar_tuple(bar: Bar) -> tuple:
    return bar.sid, bar_shape(bar), bar.green, bar.red


def prioritize_bars(target_bar: Bar, trigger_bar: Bar) -> int | None:
    target_bar = bar_tuple(target_bar)
    trigger_bar = bar_tuple(trigger_bar)

    match target_bar, trigger_bar:
        case (_, _, _, _), ('2U', 'shooter', False, True):
            return 1
        case (_, _, _, _), ('2D', 'hammer', True, False):
            return 1
        case ('1', _, _, _), ('1', _, _, _):
            return 1
        case (_, _, _, _), ('2U', 'shooter', _, _):
            return 2
        case (_, _, _, _), ('2D', 'hammer', _, _):
            return 2
        case (_, _, _, _), ('2U', _, False, True):
            return 2
        case (_, _, _, _), ('2D', _, True, False):
            return 2
        case (_, _, _, _), ('2U', _, _, _):
            return 3
        case (_, _, _, _), ('2D', _, _, _):
            return 3
        case (_, _, _, _), ('3', _, _, _):
            return 4
        case ('1', _, _, _), (_, _, _, _):
            return 4
        case (_, _, _, _), ('1', _, _, _):
            return 4
        case ('3', _, _, _), (_, _, _, _):
            return 4


def build_setup(symbol: str, bar_series: BarSeries) -> SetupMsg | None:
    target_bar, trigger_bar = bar_series.strat_candles()
    if target_bar is None or trigger_bar is None:
        return None

    setup = SetupMsg(
        symbol=symbol,
        timestamp=datetime.fromtimestamp(trigger_bar.ts, tz=timezone.utc),
        tf=bar_series.tf,
        trigger_bar=trigger_bar,
        target_bar=target_bar,
        current_bar=bar_series.get_newest(),
        pattern=[target_bar.sid, trigger_bar.sid],
        priority=prioritize_bars(target_bar, trigger_bar),
        shape=bar_shape(trigger_bar),
    )
    # print(datetime.now(), f'new candle pair: {symbol} - {setup.pattern}')
    return setup


# @dataclass
# class SetupGroup:
#     symbol: str
#     price: float
#     tf_bar_series: dict[str, BarSeries]
#     historical_setups: dict[str, Setup]


# def opening_prices(tf_bar_series):
#     opens = {}
#     for tf, bar_series in tf_bar_series.items():
#         current_bar = bar_series.get_newest()
#         opens[tf] = Decimal(str(current_bar.o))
#     return opens


# def tfc_state(tf_bar_series):
#     opens = {}
#     for tf, bars_ in tf_bar_series.items():
#         open_ = bars_[-1]['o']
#         close = bars_[-1]['c']
#         if close > open_:
#             direction = 1
#         elif close < open_:
#             direction = -1
#         else:
#             direction = 0
#         opens[tf] = direction


# @dataclass
# class TFCState:
#     open: Decimal
#     distance_usd: Decimal
#     distance_ratio: Decimal
#     distance_percent: Decimal


# def tfc_state(opens, last_price):
#     tfc_table = {}
#     for tf, open_price in opens.items():
#         distance_usd = last_price - open_price
#         distance_ratio = round(distance_usd / open_price, 3)
#         distance_percent = round(distance_ratio * 100, 3)
#         tfc_table[tf] = TFCState(
#             open=open_price,
#             distance_usd=distance_usd,
#             distance_ratio=distance_ratio,
#             distance_percent=distance_percent
#         )
#     return tfc_table


def create_setups_from_bar_series(setups, tf_bar_series):
    if setups is None:
        setups = {}

    for tf, bar_series in tf_bar_series.items():
        previous_bar = bar_series.get_previous()
        if previous_bar is None:
            continue
        prev_bar_timestamp = datetime.fromtimestamp(previous_bar.ts, tz=timezone.utc)
        if setups.get(tf) is None or setups[tf].timestamp < prev_bar_timestamp:
            setups[tf] = build_setup(bar_series.symbol, bar_series)
        else:
            setups[tf].current_bar = bar_series.get_newest()
    return setups, copy.deepcopy(setups)


def find_targets(setup, bar_series):
    high_or_low = 'h' if setup.direction == 1 else 'l'
    if setup.potential_outside:
        target = getattr(setup.trigger_bar, high_or_low)
    elif setup.pattern in [['1', '2U'], ['1', '1'], ['1', '2D']]:
        try:
            bar = bar_series.get_by_index(-4)
            target = getattr(bar, high_or_low)
        except IndexError:
            target = getattr(setup.target_bar, high_or_low)
    else:
        target = getattr(setup.target_bar, high_or_low)
    return target

