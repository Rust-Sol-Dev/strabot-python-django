from decimal import Decimal
from dataclasses import dataclass

import pandas as pd


@dataclass
class CandlePair:
    trigger_candle: pd.Series
    target_candle: pd.Series

    # price: float = None

    # @property
    # def inside(self):
    #     return (self.target_candle.high >= self.trigger_candle.high) and (
    #                 self.target_candle.low <= self.trigger_candle.low)
    #
    # @property
    # def two_up(self):
    #     return self.target_candle.high < self.trigger_candle.high
    #
    # @property
    # def two_down(self):
    #     return self.target_candle.low > self.trigger_candle.low
    #
    # @property
    # def outside(self):
    #     return self.target_candle.high < self.trigger_candle.high and self.target_candle.low > self.trigger_candle.low

    # @property
    # def potential_outside(self) -> tuple[bool, int]:
    #     """returns: bool, direction: int (0 if False)"""
    #     if not self.price:
    #         return False, 0
    #     upward = self.price > self.outside_trigger and self.trigger_candle.low < self.target_candle.low
    #     downward = self.price < self.outside_trigger and self.trigger_candle.high > self.target_candle.high
    #     if upward:
    #         return True, 1
    #     elif downward:
    #         return True, -1
    #     return False, 0
    #
    # @property
    # def is_potential_outside(self) -> bool:
    #     return self.potential_outside[0]
    #
    # @property
    # def potential_outside_direction(self) -> int:
    #     return self.potential_outside[1]

    # @property
    # def strat_id(self) -> str:
    #     if self.inside:
    #         return '1'
    #     elif self.outside:
    #         return '3'
    #     elif self.two_up:
    #         return '2U'
    #     elif self.two_down:
    #         return '2D'
    #     else:
    #         raise ValueError('unable to determine strat_id')

    @property
    def strat_pattern(self) -> list:
        # if self.is_potential_outside:
        #     return [self.target_candle.strat_id, 'P3']
        # else:
        return [self.target_candle.strat_id, self.trigger_candle.strat_id]

    @property
    def outside_trigger(self):
        high = Decimal(str(self.target_candle.high))
        low = Decimal(str(self.target_candle.low))
        return float((high + low) / 2)

    @property
    def trigger_candle_direction(self) -> int:
        candle_close = self.trigger_candle.close
        candle_range = self.trigger_candle.high - self.trigger_candle.low
        direction = 0
        if candle_close > self.trigger_candle.low + (candle_range * 0.8):
            direction = 1
        elif candle_close < self.trigger_candle.high - (candle_range * 0.8):
            direction = -1
        return direction

    @staticmethod
    def _bar_info(bar: pd.Series) -> tuple:
        return (
            bar.strat_id,
            bar.candle_shape,
            bool(bar.green),
            bool(bar.red)
        )

    def prioritize_setup(self) -> int | None:
        target_bar = self._bar_info(self.target_candle)
        trigger_bar = self._bar_info(self.trigger_candle)

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
                return 3
            case ('1', _, _, _), (_, _, _, _):
                # if self.trigger_candle.strat_id != '3':
                return 3
            case (_, _, _, _), ('1', _, _, _):
                return 3
            case ('3', _, _, _), (_, _, _, _):
                # if self.trigger_candle.strat_id != '3':
                return 3

    # @property
    # def setup_timestamp(self) -> datetime:
    #     return self.trigger_candle.iloc[-1].name

    @property
    def candle_tag(self) -> str:
        trigger_candle = self.trigger_candle.to_dict()
        return trigger_candle['candle_shape']

    @property
    def stop(self) -> Decimal:
        high = Decimal(str(self.trigger_candle.high))
        low = Decimal(str(self.trigger_candle.low))
        midprice = high - ((high - low) / 2)
        return Decimal(str(midprice))

# def candle_pair_from_dict(trigger_candle: dict, target_candle: dict) -> CandlePair:
#     key_mapping = {
#         'ts': 'time',
#         'o': 'open',
#         'h': 'high',
#         'l': 'low',
#         'c': 'close',
#         'v': 'volume',
#         'sid': 'strat_id'
#     }
#
#     trigger_candle = {key_mapping[key]: value for key, value in trigger_candle.items()}
#     trigger_candle['time'] = pd.to_datetime(trigger_candle['time'], unit='s')
#
#     target_candle = {key_mapping[key]: value for key, value in target_candle.items()}
#     target_candle['time'] = pd.to_datetime(target_candle['time'], unit='s')
#
#     trigger_candle = pd.Series(trigger_candle)
#     target_candle = pd.Series(target_candle)
#     return CandlePair(trigger_candle, target_candle)
