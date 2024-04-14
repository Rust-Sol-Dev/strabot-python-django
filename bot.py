from __future__ import annotations

import argparse
import sys
import os
import logging
from collections import defaultdict
from logging.handlers import TimedRotatingFileHandler
from datetime import timedelta, datetime
from time import perf_counter, sleep
from typing import Optional

import msgspec
import pandas as pd
import pytz
from django.db.models import Q
from orjson import orjson

from stratbot.scanner.ops import candles

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings.prod")
import django
django.setup()
from django.utils import timezone
from django.conf import settings
from django.db import IntegrityError
from django.core.cache import caches

from dotenv import load_dotenv
from rich.logging import RichHandler

from stratbot.alerts import tasks
from stratbot.alerts.ops.user_setup_alerts import create_user_setup_alert, AlertType
from stratbot.scanner.models.symbols import SymbolRec, Setup, SetupBuilder, NegatedReason, Direction
from stratbot.users.models import User
from v1.engines import CryptoEngine, StockEngine
from stratbot.scanner.ops.candles.candlepair import CandlePair


handler = TimedRotatingFileHandler('bot.log', when='midnight', backupCount=5)
handler.setFormatter(logging.Formatter('%(asctime)s : %(levelname)s : %(threadName)s : %(funcName)s : %(module)s : %(message)s'))

log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)
log.propagate = False
log.addHandler(handler)

rich_handler = RichHandler(show_time=False, show_path=False, rich_tracebacks=True)
rich_handler.setFormatter(logging.Formatter('%(asctime)s : %(threadName)s : %(funcName)s : %(module)s : %(message)s'))
log.addHandler(rich_handler)


load_dotenv(dotenv_path='v1/.env')
dev = bool(os.getenv("DEV") == 'True')

cache = caches['markets']
r = cache.client.get_client(write=True)


indicies = [
    'SPY', 'QQQ',
    'XBI', 'XLB', 'XLC', 'XLE', 'XLF', 'XLI', 'XLK', 'XLP', 'XLRE', 'XLU', 'XLV',
    'BTCUSDT', 'ETHUSDT'
]

fields_to_update = [
    'has_triggered',
    'in_force',
    'in_force_alerted',
    'in_force_last_alerted',
    'hit_magnitude',
    'magnitude_alerted',
    'magnitude_last_alerted',
    'initial_trigger',
    'last_triggered',
    'in_force_alerted',
    'discord_alerted',
    'potential_outside',
]


def fetch_bars(scanner: StockEngine | CryptoEngine):
    symbols = (
        SymbolRec.objects
        .filter(symbol_type=scanner.engine_type)
        .values_list('symbol', flat=True)
    )

    bars = dict()
    with r.pipeline() as pipe:
        for symbol in symbols:
            pipe.json().get(f'barHistory:{scanner.engine_type}:{symbol}')

        for symbol, data in zip(symbols, pipe.execute()):
            if data:
                bars[symbol] = data
            else:
                log.warning(f'no data for symbol: {symbol}')
                pass

    return bars


def process_gappers(scanner: StockEngine | CryptoEngine):
    setups_to_update = []
    for setup in scanner.setups:
        if setup.tf not in ['15', '30', '60', '4H', 'D']:
            continue

        symbol = setup.symbol_rec.symbol
        price = setup.symbol_rec.price

        is_gapping = setup.is_gapping(setup.symbol_rec.price)
        if setup.gapped and not is_gapping:
            log.info(f"no longer gapping: {symbol}, {setup}")
            setup.gapped = False
            setups_to_update.append(setup)
        elif is_gapping and not setup.gapped:
            gap_percentage = (price - setup.trigger) / setup.trigger
            log.info(f"gapping {gap_percentage}%: {symbol}, {setup}")
            setup.gapped = True
            setups_to_update.append(setup)
    Setup.objects.bulk_update(setups_to_update, ['gapped'])


def is_in_force(setup: Setup) -> bool:
    price = setup.symbol_rec.price
    triggered_bear = setup.direction == -1 and price < setup.trigger
    triggered_bull = setup.direction == 1 and price > setup.trigger
    return triggered_bear or triggered_bull


def scan_loop(scanner: StockEngine | CryptoEngine, setups: list[Setup]):
    loop_timer = perf_counter()

    if isinstance(scanner, StockEngine) and scanner.exchange_calendar.is_closed():
        if scanner.exchange_calendar.is_premarket_hours():
            process_gappers(scanner)
            elapsed = perf_counter() - loop_timer
            log.info(f'scanned gappers in {elapsed * 1000:.4f} ms ({elapsed:.2f} s)')
        else:
            sleep(0.5)
        return

    bars = fetch_bars(scanner)
    user = User.objects.get(email='tom@bluegravity.com')

    setups_to_update = []
    for setup in setups:
        symbol = setup.symbol_rec.symbol
        price = setup.symbol_rec.price
        if not price:
            continue

        tf = setup.tf
        updated = False

        # ==============================================================================================================
        # check negatable conditions
        # ==============================================================================================================

        # check if gapping > 0.50%
        # if setup.tf in ['15', '30', '60', '4H', 'D'] and setup.gapped is True:
        #     gap_percentage = (price - setup.trigger) / setup.trigger
        #     if abs(gap_percentage) > 0.005:
        #         log.info(f'gapping > 0.5%, negating: {symbol}, {setup}')
        #         setup.negated = True
        #         negated_reason = NegatedReason.objects.get(reason=NegatedReason.GAPPING)
        #         setup.negated_reasons.add(negated_reason)

        # check magnitude threshold
        # if symbol not in indicies and setup.below_mag_threshold and setup.potential_outside is False and setup.tf != '60':
        #     log.info(
        #         f"magnitude ({setup.magnitude_percent}) < threshold ({setup.mag_threshold}), negating: {symbol}, {setup}"
        #     )
        #     setup.negated = True
        #     negated_reason = NegatedReason.objects.get(reason=NegatedReason.MAG_THRESHOLD)
        #     setup.negated_reasons.add(negated_reason)

        # check rr threshold
        # rr_threshold = 1.0
        # if symbol not in indicies and not setup.potential_outside and setup.rr < rr_threshold:
        #     log.info(f"rr: {setup.rr} < {rr_threshold}, negating: {symbol}, {setup}")
        #     setup.negated = True
        #     setup.save()
        #     continue

        # remove setups on smaller timeframes moving against TFC
        if setup.tf in ["15", "30"] and setup.potential_outside is False:
            try:
                daily_open = bars[symbol]['D'][-1]['o']
            except (KeyError, IndexError):
                log.warning(f'no daily open for {symbol}, {tf}')
                continue

            bull_day_bear_setup = setup.direction == -1 and setup.trigger > daily_open
            bear_day_bull_setup = setup.direction == 1 and setup.trigger < daily_open

            if bull_day_bear_setup or bear_day_bull_setup:
                log.info(f"TFC mismatch (daily): {symbol}, {setup}")
                setup.negated = True
                negated_reason = NegatedReason.objects.get(reason=NegatedReason.TFC_CONFLICT)
                setup.negated_reasons.add(negated_reason)

        if setup.negated:
            setup.save()
            continue

        # ==============================================================================================================

        if is_in_force(setup):
            setup.in_force = True

            if not setup.initial_trigger:
                log.info(f"initial trigger: {symbol}, {setup}")
                setup.initial_trigger = timezone.now()
                # opposing_setup = Setup.objects.get(symbol_rec=setup.symbol_rec, tf=setup.tf, direction=-setup.direction)
                # print(opposing_setup)

            setup.last_triggered = timezone.now()
            updated = True
        else:
            setup.in_force = False
            updated = True

        try:
            target = setup.targets[0]
        except IndexError:
            # print(setup.symbol_rec.symbol, setup.tf, setup.direction)
            continue

        # try:
        #     df = pd.DataFrame(bars[symbol][tf])
        #     df['time'] = pd.to_datetime(df['ts'], unit='s', utc=True)
        #     df.set_index('time', inplace=True)
        #     df.rename(columns={
        #         'o': 'open',
        #         'h': 'high',
        #         'l': 'low',
        #         'c': 'close',
        #         'v': 'volume',
        #         'sid': 'strat_id',
        #     }, inplace=True)
        #     df = candles.stratify_df(df)
        #
        #     previous_bar = df.iloc[-2]
        #     current_bar = df.iloc[-1]
        #     candle_pair = CandlePair(previous_bar, current_bar)
        #     # price = current_bar['c']
        # except (KeyError, IndexError):
        #     log.warning(f'no bar data for {symbol}, {tf}')
        #     continue

        # check if hit magnitude
        try:
            current_bar = bars[symbol][tf][-1]
            hit_magnitude_bull = setup.direction == 1 and (price >= target or current_bar['h'] >= target)
            hit_magnitude_bear = setup.direction == -1 and (price <= target or current_bar['l'] <= target)
            if not setup.hit_magnitude and (hit_magnitude_bull or hit_magnitude_bear):
                log.info(f"hit magnitude: {symbol}, {setup}")
                setup.hit_magnitude = True
                updated = True
        except (KeyError, IndexError) as e:
            print(e)
            pass

        # check for potential outside bar
        # if tf not in ["15", "30"] and not setup.potential_outside:
        #     if candle_pair.is_potential_outside:
        #         direction = candle_pair.potential_outside_direction
        #         builder = SetupBuilder(setup.symbolrec, tf, direction, df)
        #         outside_bar_setup = (
        #             builder
        #             .with_candle_pair(candle_pair)
        #             .with_priority(2)
        #             .with_rr_min(0)
        #             .with_potential_outside_bar(setup.expires)
        #             .build()
        #         )
        #         try:
        #             outside_bar_setup.save()
        #         except IntegrityError:
        #             log.info(f"existing P3: {symbol}, {outside_bar_setup}")
        #         setup.potential_outside = True
        #         setup.negated = True
        #         setup.negated_reasons.append(NegatedReasons.POTENTIAL_OUTSIDE)
        #         updated = True
        #         related_setups = Setup.objects.filter(
        #             ~Q(pk=outside_bar_setup.pk),
        #             symbol_rec=setup.symbol_rec,
        #             timestamp=setup.timestamp,
        #             tf=setup.tf,
        #             pattern=setup.pattern,
        #         )
        #         for related_setup in related_setups:
        #             related_setup.negated = True
        #             related_setup.negated_reasons.append(NegatedReasons.POTENTIAL_OUTSIDE)
        #             related_setup.save()
        #             log.info(f"negating setup (potential outside): {symbol}, {related_setup}")

        if (setup.in_force and not setup.in_force_alerted) or (setup.hit_magnitude and not setup.magnitude_alerted):
            alert_type = None
            if setup.in_force:
                alert_type = AlertType.IN_FORCE
                setup.in_force_alerted = True
                setup.in_force_last_alerted = timezone.now()
                setup.save()

            if setup.hit_magnitude:
                alert_type = AlertType.MAGNITUDE
                setup.magnitude_alerted = True
                setup.magnitude_last_alerted = timezone.now()
                setup.save()

            # todo: temp check start_datetime against initial_trigger to skip dupes after a code restart
            if setup.initial_trigger and scanner.started_on < setup.initial_trigger - timedelta(seconds=60):
                # scanner.console.add_row(symbolrec, setup)
                # log.info(f'{setup.symbol_rec.symbol}, {setup}')
                # symbolrec = SymbolRec.objects.get(symbol=symbol)

                setup_msg = f'{symbol} - [{setup.tf}] {setup.pattern} {setup.direction} {setup.candle_tag}'
                payload = {
                    'timestamp': setup.initial_trigger.strftime('%Y-%m-%d %H:%M:%S'),
                    'symbol': symbol,
                    'tf': setup.tf,
                    'pattern': ' - '.join(setup.pattern),
                    'direction': Direction(setup.direction).label,
                    'candle_tag': setup.candle_tag,
                    'symbol_type': setup.symbol_rec.symbol_type,
                    # 'msg': setup_msg,
                }
                # create_user_setup_alert(
                #    user=user,
                #    symbol_rec=setup.symbol_rec,
                #    setup=setup,
                #    alert_type=alert_type,
                #    data=payload,
                #    persist=True,
                # )
                # if not dev:
                #     # tasks.send_discord_alert.delay(symbolrec.pk, setup.pk)
                #     if setup.tf not in ['15', '30'] and setup.symbol_rec.skip_discord_alerts is False:
                #         tasks.send_discord_alert.delay(setup.symbol_rec.pk, setup.pk)
                # else:
                #     log.info(f"skipping discord alert: {scanner.started_on=}, {setup.initial_trigger=}")

        if updated is True:
            setups_to_update.append(setup)
    Setup.objects.bulk_update(setups_to_update, fields_to_update)
    elapsed = perf_counter() - loop_timer
    log.info(f'scanned {scanner.timeframes} - {len(scanner.setups)} setups in {elapsed * 1000:.4f} ms ({elapsed:.2f} s)')
    # if scanner.console.table.row_count > 0:
    #     print(scanner.console.table)
    #     print('\a') # beep


def main(scanner: Optional[StockEngine, CryptoEngine]):
    setups = scanner.setups
    last_setup_refresh = timezone.now()

    while True:
        if last_setup_refresh < timezone.now() - timedelta(seconds=5):
            setups = scanner.setups
            last_setup_refresh = timezone.now()
        scan_loop(scanner, setups)


VALID_TIMEFRAMES = {'15', '30', '60', '4H', '6H', '12H', 'D', 'W', 'M', 'Q', 'Y'}


def parse_timeframes(timeframe_string):
    tfs = timeframe_string.split(',')
    if all(tf in VALID_TIMEFRAMES for tf in tfs):
        return tfs
    else:
        raise argparse.ArgumentTypeError(f"Timeframes must be one or more of {', '.join(VALID_TIMEFRAMES)}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run the stock or crypto engine.")

    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--stocks", action="store_true", help="Use the Stock Engine")
    group.add_argument("--crypto", action="store_true", help="Use the Crypto Engine")
    parser.add_argument("--timeframes", type=parse_timeframes, required=True, help="CSV list of timeframes")

    args = parser.parse_args()
    timeframes = args.timeframes

    engine = None
    if args.stocks:
        engine = StockEngine(timeframes)
    elif args.crypto:
        engine = CryptoEngine(timeframes)

    main(engine)