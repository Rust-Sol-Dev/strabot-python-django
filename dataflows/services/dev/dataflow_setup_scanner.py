from decimal import Decimal
from time import perf_counter
import os
from collections import defaultdict
from datetime import datetime, timezone

import bytewax.operators as op
import pandas as pd
import redis
from bytewax.connectors.stdio import StdOutSink
from bytewax.dataflow import Dataflow
from bytewax.connectors.kafka import KafkaSource, KafkaSink
from bytewax.outputs import DynamicSink, StatelessSinkPartition
from confluent_kafka import OFFSET_END
import msgspec
from django.db import IntegrityError
from django.db.models import Q

from dataflows.serializers import deserialize, serialize
from stratbot.scanner.ops import candles
from dataflows import bars

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings.dev")
import django
django.setup()
from django.conf import settings
from stratbot.scanner.models.symbols import Setup, NegatedReason, SymbolRec


potential_outside_bars = defaultdict(dict)
negated_reason = NegatedReason.objects.get(reason=NegatedReason.POTENTIAL_OUTSIDE)
r = redis.Redis(host=settings.REDIS_HOST, port=6379, db=11)


kafka_conf = {
    'security.protocol': settings.REDPANDA_SECURITY_PROTOCOL,
    'sasl.mechanism': settings.REDPANDA_SASL_MECHANISM,
    'sasl.username': settings.REDPANDA_USERNAME,
    'sasl.password': settings.REDPANDA_PASSWORD,
    'group.id': 'bytewax-potential-outside-consumer',
    'enable.auto.commit': True,
}

kafka_source = KafkaSource(
    brokers=settings.REDPANDA_BROKERS,
    topics=['ALPACA.bars_resampled', 'BINANCE.bars_resampled'],
    starting_offset=OFFSET_END,
    add_config=kafka_conf,
    batch_size=2500,
)

kafka_sink = KafkaSink(
    brokers=settings.REDPANDA_BROKERS,
    topic='setups',
    add_config=kafka_conf,
)


class SetupPartition(StatelessSinkPartition):

    def write_batch(self, batch: list) -> None:
        for symbol, potential_outside_setup in batch:
            try:
                potential_outside_setup.save()
                print(symbol, potential_outside_setup)
            except IntegrityError as e:
                # print(e)
                pass

            setups = (
                Setup.objects
                .filter(
                    ~Q(id=potential_outside_setup.pk),
                    ~Q(pattern__1='P3'),
                    Q(direction=potential_outside_setup.direction),
                    symbol_rec__symbol=symbol,
                    tf=potential_outside_setup.tf,
                    negated=False,
                    expires__gt=datetime.now(tz=timezone.utc)
                )
            )

            for setup in setups:
                setup.negated = True
                setup.negated_reasons.add(negated_reason)
                setup.potential_outside = True
                setup.save()
                print(f'negated: {symbol} - {setup}')


class SetupSink(DynamicSink):
    def build(self, worker_index, worker_count) -> SetupPartition:
        return SetupPartition()


class SetupMsg(msgspec.Struct):
    timestamp: datetime
    tf: str
    pattern: list[str]
    trigger_candle: dict
    target_candle: dict
    trigger: float
    direction: int
    has_triggered: bool = False
    initial_trigger: datetime | None = None
    in_force: bool = False
    potential_outside: bool = False


"""
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
    # potential_outside_initial_trigger = models.DateTimeField(
    #     "Potential Outside Initial Trigger", blank=True, null=True, default=None
    # )
    discord_alerted = models.BooleanField("Discord Alerted", default=False)
    gapped = models.BooleanField("Gapped?", default=False)
    negated = models.BooleanField("Negated", default=False)
    negated_reasons = ArrayField(
        models.IntegerField(choices=NegatedReasons.choices), null=True,blank=True, default=list
    )
"""


def flat_map_bars(symbol__value):
    symbol, value = symbol__value

    results = []
    for tf, bars_ in value.items():
        if tf in ['15', '30']:
            continue
        previous_bar, current_bar = bars_[-2:]
        previous_bar = bars.Bar(**previous_bar)
        current_bar = bars.Bar(**current_bar)
        results.append((symbol, tf, [previous_bar, current_bar]))
    return results


# def bars_to_dataframe(symbol__tf__bars):
#     symbol, tf, bars = symbol__tf__bars
#
#     s = perf_counter()
#     df = pd.DataFrame(bars)
#     df['time'] = pd.to_datetime(df['ts'], unit='s', utc=True)
#     # df.set_index('time', inplace=True)
#     df.rename(columns={
#         'o': 'open',
#         'h': 'high',
#         'l': 'low',
#         'c': 'close',
#         'v': 'volume',
#         'sid': 'strat_id',
#     }, inplace=True)
#     df = candles.stratify_df(df)
#     elapsed = perf_counter() - s
#     print(f'completed in {elapsed * 1000:.4f} ms ({elapsed:.2f} s)')
#     return symbol, tf, df


def check_setups(symbol__tf__bars):
    pass


def is_potential_outside(symbol__tf__bars):
    symbol, tf, bars_ = symbol__tf__bars
    previous_bar, current_bar = bars_

    try:
        ts = potential_outside_bars[symbol][tf]
    except KeyError:
        ts = None

    if current_bar.ts == ts or current_bar.sid == '3':
        return

    price = current_bar.c
    high = Decimal(str(previous_bar.h))
    low = Decimal(str(previous_bar.l))
    outside_trigger = (high + low) / 2
    upward = price > outside_trigger and current_bar.l < previous_bar.l
    downward = price < outside_trigger and current_bar.h > previous_bar.h
    direction = 1 if upward else -1 if downward else 0

    if upward or downward:
        ts = datetime.fromtimestamp(current_bar.ts, tz=timezone.utc)
        # setupmsg = SetupMsg(
        #     timestamp=ts,
        #     tf=tf,
        #     trigger_candle=previous_bar.as_dict,
        #     target_candle=previous_bar.as_dict,
        #     pattern=[previous_bar.sid, 'P3'],
        #     trigger=outside_trigger,
        #     has_triggered=True,
        #     initial_trigger=datetime.now(tz=timezone.utc),
        #     in_force=True,
        #     direction=direction,
        #     potential_outside=True,
        # )

        symbolrec = SymbolRec.objects.get(symbol=symbol)
        potential_outside_setup = Setup(
            symbol_rec=symbolrec,
            timestamp=ts,
            expires=ts + symbolrec.CANDLE_EXPIRE_DELTAS[tf],
            tf=tf,
            trigger_candle=previous_bar.as_dict,
            target_candle=previous_bar.as_dict,
            pattern=[previous_bar.sid, 'P3'],
            trigger=outside_trigger,
            has_triggered=True,
            initial_trigger=datetime.now(tz=timezone.utc),
            targets=[previous_bar.h] if direction == 1 else [previous_bar.l],
            stop=previous_bar.l if direction == 1 else previous_bar.h,
            rr=1,
            in_force=True,
            direction=direction,
            potential_outside=True,
        )

        potential_outside_bars[symbol][tf] = current_bar.ts

        return symbol, potential_outside_setup


# ======================================================================================================================
# START DATAFLOW
# ======================================================================================================================

flow = Dataflow('potential_outside')

stream = (
    op.input('kafka_source', flow, kafka_source)
    .then(op.map, 'deserialize', deserialize)
    # .then(op.filter_map, 'check_setups', check_setups)
    .then(op.flat_map, 'flat_map_bars', flat_map_bars)
    # .then(op.map, 'bars_to_dataframe', bars_to_dataframe)
    # .then(op.filter_map, 'is_potential_outside', is_potential_outside)
    # .then(op.map, 'serialize', serialize)
)
op.output('stdout_sink', stream, StdOutSink())
# op.output('kafka_sink', stream, kafka_sink)
# op.output('setup_sink', stream, SetupSink())
