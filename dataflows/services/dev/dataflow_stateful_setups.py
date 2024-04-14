import os
import copy
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Tuple, Callable

import bytewax.operators as op
import bytewax.operators.window as window_op
import msgspec
from bytewax.connectors.stdio import StdOutSink
from bytewax.dataflow import Dataflow
from bytewax.connectors.kafka import KafkaSource
from bytewax.operators.window import EventClockConfig, TumblingWindow
from bytewax.outputs import DynamicSink, StatelessSinkPartition
from bytewax.testing import TestingSource

from dataflows.serializers import deserialize

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings.dev")
import django
django.setup()
from django.conf import settings
from django.core.cache import caches
from stratbot.scanner.models.symbols import SymbolRec

from dataflows.bars import Bar, BarSeries, to_ohlc, strat_id, bar_shape, potential_outside_bar, to_bar_series_by_tf, \
    add_key_to_value
from dataflows.timeframe_ops import floor_datetime_fixed, floor_datetime_variable

cache = caches['markets']
r = cache.client.get_client(write=True)
# bar_history_key_prefix = 'barHistory:crypto:'


kafka_conf = {
    'security.protocol': settings.REDPANDA_SECURITY_PROTOCOL,
    'sasl.mechanism': settings.REDPANDA_SASL_MECHANISM,
    'sasl.username': settings.REDPANDA_USERNAME,
    'sasl.password': settings.REDPANDA_PASSWORD,
    'group.id': 'bytewax-stateful-consumer',
    # 'enable.auto.commit': True,
}

kafka_source = KafkaSource(
    brokers=settings.REDPANDA_BROKERS,
    topics=['BINANCE.bars_resampled'],
    # topics=['ALPACA.bars_resampled'],
    add_config=kafka_conf,
    batch_size=5000,
)


class SetupMsg(msgspec.Struct):
    timestamp: datetime
    tf: str
    pattern: list[str]
    trigger_bar: dict
    target_bar: dict
    direction: int = 0
    initial_trigger: datetime | None = None
    initial_outside_trigger: datetime | None = None
    has_triggered: bool = False
    trigger_count: int = 0
    in_force: bool = False
    in_force_alerted: bool = False
    in_force_last_alerted: datetime | None = None
    hit_magnitude: bool = False
    magnitude_alerted: bool = False
    magnitude_last_alerted: datetime | None = None
    priority: int | None = None
    shape: str | None = None
    potential_outside: bool = False
    potential_outside_direction: int = 0

    negated: bool = False
    negated_reasons: set[str] = set()

    def __str__(self):
        return f'{self.timestamp} [{self.tf}] {self.pattern} [{self.shape}]'

    def bull_trigger(self):
        return Decimal(self.trigger_bar['h'])

    def bear_trigger(self):
        return Decimal(self.trigger_bar['l'])

    def outside_trigger(self):
        return (self.bear_trigger() + self.bull_trigger()) / 2


def _bar_info(bar: Bar) -> tuple:
    return bar.sid, bar_shape(bar), bar.green, bar.red


def prioritize_setup(target_bar: Bar, trigger_bar: Bar) -> int | None:
    target_bar = _bar_info(target_bar)
    trigger_bar = _bar_info(trigger_bar)

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
            return 3
        case (_, _, _, _), ('1', _, _, _):
            return 3
        case ('3', _, _, _), (_, _, _, _):
            return 3


def build_setupmsg(symbol: str, bar_series: BarSeries) -> SetupMsg | None:
    target_bar, trigger_bar = bar_series.strat_candles()
    if target_bar is None or trigger_bar is None:
        return None

    setup = SetupMsg(
        timestamp=datetime.fromtimestamp(trigger_bar.ts, tz=timezone.utc),
        tf=bar_series.tf,
        trigger_bar=trigger_bar,
        target_bar=target_bar,
        pattern=[target_bar.sid, trigger_bar.sid],
        priority=prioritize_setup(target_bar, trigger_bar),
        shape=bar_shape(trigger_bar),
    )
    print(datetime.now(), f'new candle pair: {symbol} - {setup.pattern}')
    return setup


def scan_bars(historical_setups, symbol__tf_bar_series):
    symbol, tf_bar_series = symbol__tf_bar_series
    # last_price = Decimal(str(tf_bar_series['15'].get_newest().c))
    # opens = opening_prices(tf_bar_series)
    # tfc_table = tfc_state(opens, last_price)

    if historical_setups is None:
        historical_setups = {}
        for tf, bar_series in tf_bar_series.items():
            historical_setups[tf] = build_setupmsg(symbol, bar_series)

    for tf, setup in historical_setups.items():
        bar_series = tf_bar_series[tf]
        target_bar, trigger_bar = bar_series.strat_candles()
        if target_bar is None or trigger_bar is None:
            continue

        trigger_bar_ts = datetime.fromtimestamp(trigger_bar.ts, tz=timezone.utc)
        if trigger_bar_ts != setup.timestamp:
            historical_setups[tf] = build_setupmsg(symbol, bar_series)
            # continue

        # if setup.hit_magnitude:
        #     continue

        # current_bar = bar_series.get_newest()

        # # check if potential outside bar
        # if setup.potential_outside is False and tf not in ['15', '30']:
        #     potential_outside, direction = potential_outside_bar(trigger_bar, current_bar)
        #     if potential_outside:
        #         setup.potential_outside = True
        #         # setup.potential_outside_direction = direction
        #         if not setup.initial_outside_trigger:
        #             setup.initial_outside_trigger = datetime.now(tz=timezone.utc)
        #         print(f'* POTENTIAL OUTSIDE: {symbol} {tf} {setup.pattern} {direction=} {current_bar.sid} {setup.priority} {setup.shape}')
        #
        # if setup.potential_outside is True:
        #     if current_bar.sid == '3':
        #         setup.hit_magnitude = True
        #         print(f'* HIT MAGNITUDE: {symbol} {tf} {setup.pattern} {current_bar.sid} {setup.priority} {setup.shape}')
        #         continue
        #     else:
        #         outside_trigger = (Decimal(str(trigger_bar.h)) + Decimal(str(trigger_bar.l))) / 2
        #         in_force_bear = setup.potential_outside_direction == -1 and current_bar.c <= outside_trigger
        #         in_force_bull = setup.potential_outside_direction == 1 and current_bar.c >= outside_trigger
        # else:
        #     in_force_bear = current_bar.c < trigger_bar.l
        #     in_force_bull = current_bar.c > trigger_bar.h
        #
        # is_in_force = in_force_bear or in_force_bull
        # if is_in_force:
        #     setup.in_force = True
        #     setup.has_triggered = True
        #     if not setup.initial_trigger:
        #         setup.initial_trigger = datetime.now(tz=timezone.utc)
        #     if in_force_bear:
        #         setup.direction = -1
        #     elif in_force_bull:
        #         setup.direction = 1
        # else:
        #     setup.in_force = False
        #
        # if (setup.in_force and not setup.in_force_alerted) or (setup.hit_magnitude and not setup.magnitude_alerted):
        #     bull_or_bear = 'BULL' if setup.direction == 1 else 'BEAR'
        #     msg = ''
        #     if setup.in_force:
        #         msg = 'IN FORCE'
        #         setup.in_force_alerted = True
        #         setup.in_force_last_alerted = datetime.now(tz=timezone.utc)
        #     elif setup.hit_magnitude:
        #         msg = 'HIT MAGNITUDE'
        #         setup.magnitude_alerted = True
        #         setup.magnitude_last_alerted = datetime.now(tz=timezone.utc)
        #
        #     print(
        #         datetime.now(),
        #         f'{msg} - {bull_or_bear}:',
        #         symbol,
        #         tf,
        #         setup.pattern,
        #         current_bar.sid,
        #         setup.priority,
        #         setup.shape,
        #         setup.potential_outside,
        #     )
        #         # if current_bar.c > max(opens.values()) or current_bar.c < min(opens.values()):
        #             # print(tfc_table)
        #     #         tfc_output = ' '.join([f"[{tf}] {tfc.distance_percent}%" for tf, tfc in tfc_table.items()])
        #     #         print('* FTFC:', tfc_output)

    return historical_setups, copy.deepcopy(historical_setups)


class _SinkPartition(StatelessSinkPartition):
    def write_batch(self, items: list) -> None:
        output = {}
        for symbol, tf_setupmsgs in items:
            for tf, setupmsg in tf_setupmsgs.items():
                output[tf] = msgspec.json.encode(setupmsg)
            # pass
        print(output)
        # print(items)
        # pass


class TestSink(DynamicSink):
    def build(self, worker_index: int, worker_count: int) -> _SinkPartition:
        return _SinkPartition()


actionable_symbols = [
    'SPY', 'QQQ', 'DIA', 'IWM',
    'XLF', 'XLE', 'XLU', 'XLV', 'XLI', 'XLB', 'XLK', 'XLC', 'XLY', 'XLRE', 'XLP', 'XBI',
    'COIN', 'TSLA', 'NVDA', 'AAPL', 'NFLX', 'WMT',
]

flow = Dataflow("example_trade_flow")
bar_stream = (
    op.input('kafka_source', flow, kafka_source)
    .then(op.map, 'deserialize', deserialize)
    # .then(op.filter, 'filter_symbols', lambda data: data[0] in actionable_symbols)
    .then(op.map, 'to_bar_series_by_tf', to_bar_series_by_tf)
    .then(op.map, 'add_key_to_bars', add_key_to_value)
)
# op.inspect('inspect', bar_stream)

s = op.stateful_map('scan_bars', bar_stream, lambda: None, scan_bars)

op.output('test_sink', s, TestSink())
# op.output('stdout_sink', bar_stream, StdOutSink())
# op.output('stdout_sink', s, StdOutSink())
