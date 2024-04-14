from __future__ import annotations

import logging
from multiprocessing import Pool
import os
import time
from abc import ABC
from collections import defaultdict, deque
from concurrent import futures
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from time import perf_counter
from typing import Any, ClassVar, Optional

import django
import pandas as pd
from django.db.models import QuerySet
from dotenv import load_dotenv
from rich.console import Console
from dirtyfields.dirtyfields import reset_state
from django.db import transaction
from django.utils import timezone

from stratbot.scanner.models.live_loop import LiveLoop as LiveLoopModel
from stratbot.scanner.models.live_loop import LiveLoopRun as LiveLoopRunModel
from stratbot.scanner.models.symbols import SymbolRec, SymbolType, Setup
from stratbot.scanner.models.timeframes import Timeframe
from stratbot.scanner.ops.candles.candlepair import CandlePair
from stratbot.scanner.ops.candles.storage import from_cache

load_dotenv(dotenv_path='v1/.env')
dev = bool(os.getenv("DEV") == 'True')
console = Console(log_time=False)


class LoopRunExit(Exception):
    """
    Indicates that we need to exit this run of a loop for some reason.
    """

    def __init__(self, reason: str, detail: str = ""):
        self.reason = reason
        self.detail = detail
        super().__init__(reason, detail)


@dataclass
class OverallLoopRunStatistics:
    loop: LiveLoop

    start_datetime: datetime
    start_perf: float
    last_datetime: datetime
    last_perf: float

    last_stats_flush_at: datetime

    last_run_number: int = 0
    num_setups_examined: int = 0
    num_setups_updated: int = 0
    num_setups_triggered: int = 0
    num_alerts_attempted: int = 0


@dataclass
class OneLoopRunStatistics:
    loop: LiveLoop
    start_datetime: datetime
    start_perf: float
    end_datetime: datetime
    end_perf: float

    run_number: int = 0
    fully_ran: bool = False
    exit_reason: str = ""
    exit_reason_detail: str = ""
    symbols_refreshed: bool = False
    symbols_refreshed_duration: float | None = None
    num_symbols: int | None = None
    setups_refreshed: bool = False
    setups_refresh_duration: float | None = None
    num_setups: int | None = None
    price_records_refreshed: bool = False
    price_records_refreshed_duration: float | None = None
    num_price_records: int | None = None
    num_setups_examined: int = 0
    num_setups_updated: int = 0
    num_setups_triggered: int = 0
    num_alerts_attempted: int = 0


@dataclass
class LastPrice:
    price: Decimal
    as_of: datetime


class LoopPersistentDataStore:
    def __init__(self, loop: LiveLoop):
        self.loop = loop
        self._symbols_needs_refresh: bool = True
        self._setups_needs_refresh: bool = True
        self._setup_mapping_needs_refresh: bool = True
        self._price_record_mapping_needs_refresh: bool = True

        self._symbolrecs: list[SymbolRec] = []
        self._setups: list[Setup] = []
        self._setup_mapping: defaultdict[
            SymbolRec, defaultdict[Timeframe, list[Setup]]
        ] = defaultdict(lambda: defaultdict(list))
        self._price_record_mapping: defaultdict[
            SymbolRec, defaultdict[Timeframe, pd.DataFrame]
        ] = defaultdict(lambda: defaultdict(pd.DataFrame))

    @property
    def any_needs_refresh(self) -> bool:
        return (
            self.symbols_needs_refresh
            or self.setups_needs_refresh
            or self.setup_mapping_needs_refresh
            or self.price_record_mapping_needs_refresh
        )

    @property
    def symbols_needs_refresh(self) -> bool:
        return self._symbols_needs_refresh

    @symbols_needs_refresh.setter
    def symbols_needs_refresh(self, value: bool) -> None:
        self._symbols_needs_refresh = value
        if value:
            self._setups_needs_refresh = value
            self._setup_mapping_needs_refresh = value
            self._price_record_mapping_needs_refresh = value

    @property
    def setups_needs_refresh(self) -> bool:
        return self._setups_needs_refresh

    @setups_needs_refresh.setter
    def setups_needs_refresh(self, value: bool) -> None:
        self._setups_needs_refresh = value
        if value:
            self._setup_mapping_needs_refresh = value

    @property
    def setup_mapping_needs_refresh(self) -> bool:
        return self._setup_mapping_needs_refresh

    @setup_mapping_needs_refresh.setter
    def setup_mapping_needs_refresh(self, value: bool) -> None:
        self._setup_mapping_needs_refresh = value

    @property
    def price_record_mapping_needs_refresh(self) -> bool:
        return self._symbols_needs_refresh

    @price_record_mapping_needs_refresh.setter
    def price_record_mapping_needs_refresh(self, value: bool) -> None:
        self._price_record_mapping_needs_refresh = value
        if value:
            self._symbols_needs_refresh = value

    @property
    def symbolrecs(self) -> list[SymbolRec]:  # dict[str, SymbolRec]:
        if self.symbols_needs_refresh:
            self._symbolrecs = list(SymbolRec.objects.filter(symbol_type=self.loop.symbol_type))
            # self._symbolrecs = {rec.symbol: rec for rec in symbolrecs}
            self.symbols_needs_refresh = False
        return self._symbolrecs

    @property
    def setups(self) -> list[Setup]:  # QuerySet:
        if self.setups_needs_refresh:
            self._setups = (
                Setup.objects
                .select_related('symbol_rec')
                .filter(expires__gt=datetime.utcnow().astimezone())
                .filter(symbol_rec__symbol_type=self.loop.symbol_type)
                .filter(negated=False)
                .filter(hit_magnitude=False)
            )
            self.setups_needs_refresh = False
        return self._setups

    @property
    def setup_mapping(self) -> defaultdict[SymbolRec, defaultdict[Timeframe, list[Setup]]]:
        if self._setup_mapping_needs_refresh:
            new_mapping: defaultdict[
                SymbolRec, defaultdict[Timeframe, list[Setup]]
            ] = defaultdict(lambda: defaultdict(list))
            symbols = self.symbolrecs
            setups = self.setups
            # Even though we have a `defaultdict`, I realized that we should pre-fill
            # this using `symbols` so that any `bulk_update` of symbols (in terms of
            # latest price, etc.) is properly reflected here since these should be the
            # symbols we're setting the latest price on.
            for symbol in symbols:
                for timeframe in self.loop.scan_timeframes:
                    new_mapping[symbol][timeframe] = []
            for setup in setups:
                setup_symbol = setup.symbol_rec
                setup_timeframe = Timeframe(setup.tf)
                new_mapping[setup_symbol][setup_timeframe].append(setup)
            self._setup_mapping = new_mapping
            self.setup_mapping_needs_refresh = False
        return self._setup_mapping

    @property
    def price_record_mapping(self) -> defaultdict[SymbolRec, defaultdict[Timeframe, pd.DataFrame]]:
        if self._price_record_mapping_needs_refresh:
            new_mapping: defaultdict[
                SymbolRec, defaultdict[Timeframe, pd.DataFrame]
            ] = defaultdict(lambda: defaultdict(pd.DataFrame))

            symbolrec_timeframe_pairs = [
                (symbolrec, timeframe)
                for symbolrec in self.symbolrecs
                for timeframe in self.loop.scan_timeframes
            ]

            for symbolrec, timeframe in symbolrec_timeframe_pairs:
                # new_mapping[symbolrec][timeframe] = getattr(symbolrec, symbolrec.TF_MAP[timeframe])
                new_mapping[symbolrec][timeframe] = pd.DataFrame()
                self.loop.logger.info(f'added {symbolrec.symbol} [{timeframe}] to pricerec mapping')

            self._price_record_mapping = new_mapping
            self.price_record_mapping_needs_refresh = False

        return self._price_record_mapping

    def check_and_refresh_all(self) -> None:
        current_stats = self.loop.current_stats
        needs_symbols = self.symbols_needs_refresh
        needs_setups = self.setups_needs_refresh
        needs_setup_mapping = self.setup_mapping_needs_refresh
        needs_price_records = self.price_record_mapping_needs_refresh

        _time = perf_counter()
        if needs_symbols:
            self.symbolrecs
            current_stats.symbols_refreshed = True
            current_stats.symbols_refreshed_duration = perf_counter() - _time
        else:
            current_stats.symbols_refreshed = False
            current_stats.symbols_refreshed_duration = None
        current_stats.num_symbols = len(self.symbolrecs)

        _time = perf_counter()
        current_stats.setups_refreshed = False
        current_stats.setups_refresh_duration = None
        if needs_setups or needs_setup_mapping:
            self.setups
            self.setup_mapping
            if needs_setups:
                current_stats.setups_refreshed = True
                current_stats.setups_refresh_duration = perf_counter() - _time
        current_stats.num_setups = len(self.setups)

        _time = perf_counter()
        if needs_price_records:
            self.price_record_mapping
            current_stats.price_records_refreshed = True
            current_stats.price_records_refreshed_duration = perf_counter() - _time
        else:
            current_stats.price_records_refreshed = False
            current_stats.price_records_refreshed_duration = None
        num_price_records: int = 0
        for parent_value in self.price_record_mapping.values():
            for child_value in parent_value.values():
                num_price_records += len(child_value)
        current_stats.num_price_records = num_price_records


class LiveLoop(ABC):
    symbol_type: ClassVar[SymbolType]
    scan_timeframes: ClassVar[tuple[Timeframe, ...]]
    display_timeframes: ClassVar[tuple[Timeframe, ...]]
    tfc_timeframes: ClassVar[tuple[Timeframe, ...]]
    logger: ClassVar[logging.Logger]

    overall_stats_class: ClassVar[type[OverallLoopRunStatistics]] = OverallLoopRunStatistics
    current_stats_class: ClassVar[type[OneLoopRunStatistics]] = OneLoopRunStatistics
    store_class: ClassVar[type[LoopPersistentDataStore]] = LoopPersistentDataStore

    def __init__(
        self,
        *,
        # The minimum duration between loop runs.
        min_wait_duration: timedelta = timedelta(seconds=1),
        # The minimum time in-between recording run statistics.
        min_stats_record_duration: timedelta = timedelta(seconds=30),
    ):
        self.overall_stats: Optional[OverallLoopRunStatistics] = None
        self.previous_stats: deque[OneLoopRunStatistics] = deque([])
        self.current_stats: Optional[OneLoopRunStatistics] = None
        self.current_datetime: Optional[datetime] = None
        self.current_perf: Optional[float] = None
        self.start_datetime: Optional[datetime] = None
        self.start_perf: Optional[float] = None
        self.current_flush_stats_db_instances: list = []
        self.overall_stats_db_instance: Optional[LiveLoopModel] = None
        self.min_wait_duration = min_wait_duration
        self.min_stats_record_duration = min_stats_record_duration
        self.store = self.store_class(self)
        self.quotes: dict[str, int] = {}

    def handle_loop_run_unhandled_exception(self, exception: Exception) -> None:
        assert not isinstance(
            exception, LoopRunExit
        ), "Should only call `handle_loop_run_exit` in this case."
        self.current_stats.exit_reason = exception.__class__.__name__
        self.current_stats.exit_reason_detail = str(exception)
        # If we run into an unhandled exception, we may have edited/modified `Setup`
        # instances. We need to refresh them so that `dirtyfields`-related logic works
        # properly, along with just, in general, model instance value consistency. If we
        # need to mark anything else here as needing refresh in the future we can
        # definitely do that as well.
        self.store.setups_needs_refresh = True
        self._handle_loop_run_unhandled_exception(exception)

    def handle_loop_run_exit(self, exit_: LoopRunExit) -> None:
        self.current_stats.exit_reason = exit_.reason
        self.current_stats.exit_reason_detail = exit_.detail
        self._handle_loop_run_exit(exit_)

    def update_stats_pre_run(self) -> None:
        self._update_stats_pre_run()

    def run_pre_run_checks(self) -> None:
        self._run_pre_run_checks()

    def check_and_refresh_setups(self) -> None:
        self.store.check_and_refresh_all()
        self._check_and_refresh_setups()

    def refresh_latest_prices(self) -> None:
        self.quotes = dict(
            SymbolRec.objects
            .filter(symbol_type=self.store.loop.symbol_type)
            .filter(as_of__gte=timezone.now() - timedelta(seconds=30))
            .values_list('symbol', 'price')
        )
        self._refresh_latest_prices()

    def run_next_iteration(self) -> None:
        self.run_pre_run_checks()
        self.check_and_refresh_setups()
        self.refresh_latest_prices()
        for timeframe in self.scan_timeframes:
            # self.check_timeframe_before_checking_setups(timeframe)
            for symbol in self.store.symbolrecs:
                for setup in self.store.setup_mapping[symbol][timeframe]:
                    self.check_setup(symbol, setup)
        self._run_next_iteration()
        self.check_and_persist_updated_setups()
        self.queue_prepared_alerts()

    def check_timeframe_before_checking_setups(self, timeframe: Timeframe) -> None:
        self._check_timeframe_before_checking_setups(timeframe)

    def check_setup(self, symbolrec: SymbolRec, setup: Setup) -> None:
        self.current_stats.num_setups_examined += 1

        symbol = symbolrec.symbol
        try:
            price = self.quotes[symbol]
        except KeyError:
            return
        tf = Timeframe(setup.tf)

        # check magnitude threshold
        if setup.below_mag_threshold:
            self.store.loop.logger.info(
                f"mag % ({setup.magnitude_percent}) < threshold ({setup.mag_threshold}), negating: {symbol}, {setup}"
            )
            setup.negated = True
            setup.save()
            return

        # check rr threshold
        rr_threshold = 1.0
        if not setup.potential_outside and setup.rr < rr_threshold:
            self.store.loop.logger.info(f"rr: {setup.rr} < {rr_threshold}, negating: {symbol}, {setup}")
            setup.negated = True
            setup.save()
            return

        # remove setups on smaller timeframes moving against TFC
        if setup.tf in ["15", "30"]:
            tfc_state = symbolrec.tfc_state(["D"])["D"]
            if (tfc_state.distance_ratio > 0 and setup.direction == -1) or (
                tfc_state.distance_ratio < 0 and setup.direction == 1
            ):
                self.store.loop.logger.info(f"TFC mismatch (daily): {symbol}, {setup}")
                setup.negated = True
                setup.save()
                return

        # is in force?
        if (setup.direction == -1 and price < setup.trigger) or (
                setup.direction == 1 and price > setup.trigger):
            setup.in_force = True
            if not setup.initial_trigger:
                self.store.loop.logger.info(f"initial trigger: {symbolrec.symbol}, {setup}")
                setup.initial_trigger = timezone.now()
            else:
                setup.last_triggered = timezone.now()
            updated = True
        elif setup.in_force is True:
            setup.in_force = False
            updated = True

        df = getattr(symbolrec, symbolrec.TF_MAP[setup.tf])
        df = df.tail(2)
        try:
            current_bar = df.iloc[-1]
            previous_bar = df.iloc[-2]
            candle_pair = CandlePair(current_bar, previous_bar, price)
        except IndexError:
            return
        target = setup.targets[0]

        # check if hit magnitude
        if ((setup.direction == 1 and (price >= target or current_bar.high >= target)) or
                (setup.direction == -1 and (price <= target or current_bar.low <= target))):
            self.store.loop.logger.info(f"hit magnitude: {symbol}, {setup}")
            setup.hit_magnitude = True
            updated = True

        # check for potential outside bar
        # if tf not in ["15", "30"] and not setup.potential_outside:
        #     if candle_pair.is_potential_outside:
        #         outside_bar_setup = symbolrec.create_setup(
        #             candle_pair=candle_pair,
        #             df=df,
        #             tf=tf,
        #             direction=candle_pair.potential_outside_direction,
        #             priority=2,
        #             rr_min=0,
        #             potential_outside=True,
        #             expires=setup.expires,
        #         )
        #         try:
        #             outside_bar_setup.has_triggered = True
        #             outside_bar_setup.initial_trigger = timezone.now()
        #             outside_bar_setup.save()
        #         except django.db.utils.IntegrityError:
        #             log.info(f"existing P3: {symbol}, {outside_bar_setup}")
        #         setup.potential_outside = True
        #         setup.negated = True
        #         updated = True
        #         related_setups = Setup.objects.filter(
        #             ~Q(pk=outside_bar_setup.pk),
        #             symbol_rec=symbolrec,
        #             timestamp=setup.timestamp,
        #             tf=setup.tf,
        #             pattern=setup.pattern,
        #         )
        #         for related_setup in related_setups:
        #             related_setup.negated = True
        #             related_setup.save()
        #             log.info(f"negating setup: {symbol}, {related_setup}")

        # if (setup.in_force and not setup.in_force_alerted) or (setup.hit_magnitude and not setup.magnitude_alerted):
        #     alert_type = None
        #     if setup.in_force:
        #         alert_type = AlertType.IN_FORCE
        #         setup.in_force_alerted = True
        #         setup.in_force_last_alerted = timezone.now()
        #         updated = True
        #
        #     if setup.hit_magnitude:
        #         alert_type = AlertType.MAGNITUDE
        #         setup.magnitude_alerted = True
        #         setup.magnitude_last_alerted = timezone.now()
        #         updated = True
        #
        #     # todo: temp check start_datetime against initial_trigger to skip dupes after a code restart
        #     if setup.initial_trigger and scanner.started_on < setup.initial_trigger - timedelta(seconds=60):
        #         # scanner.console.add_row(symbolrec, setup)
        #         log.info(f'{symbolrec}, {setup}')
        #         # user = User.objects.get(email='tom@bluegravity.com')
        #         # symbolrec = SymbolRec.objects.get(symbol=symbol)
        #         # setup_msg = f'{symbol} - [{setup.tf}] {setup.pattern} {setup.direction} {setup.candle_tag}'
        #         #create_user_setup_alert(
        #         #    user=user,
        #         #    symbol_rec=symbolrec,
        #         #    setup=setup,
        #         #    alert_type=alert_type,
        #         #    data={'msg': setup_msg},
        #         #    persist=True,
        #         #)
        #         if not dev:
        #             tasks.send_discord_alert.delay(symbolrec.pk, setup.pk)
        #             setup.discord_alerted = True
        #         else:
        #             log.info(f"skipping discord alert: {scanner.started_on=}, {setup.initial_trigger=}")

        self._check_setup(symbolrec, setup)

    def check_and_persist_updated_setups(self) -> None:
        needs_save: set[Setup] = set()
        save_fields: set[str] = set()
        for setup in self.store.setups:
            if setup.is_dirty():
                needs_save.add(setup)
                dirty_fields = setup.get_dirty_fields()
                save_fields |= set(dirty_fields)
                reset_state(sender=setup.__class__, instance=setup)
        if needs_save:
            instances = list(needs_save)
            fields = list(save_fields)
            Setup.objects.bulk_update(instances, fields, batch_size=500)
            self.current_stats.num_setups_updated += len(instances)
        self._check_and_persist_updated_setups()

    def queue_prepared_alerts(self) -> None:
        self._queue_prepared_alerts()

    def update_stats_post_run(self, exception: Exception | None) -> None:
        self.current_stats.end_datetime = timezone.now()
        self.current_stats.end_perf = perf_counter()
        self.current_stats.fully_ran = exception is None

        self.overall_stats.last_datetime = self.current_stats.end_datetime
        self.overall_stats.last_perf = self.current_stats.end_perf
        self.overall_stats.last_run_number += 1
        self.overall_stats.num_setups_examined += self.current_stats.num_setups_examined
        # NOTE: If there was an exception, the transaction rolls back. In that case, we
        # won't consider that we updated any setups or triggered any setups for the
        # overall statistics (or attempted any alerts, etc.).
        if exception is None:
            self.overall_stats.num_setups_updated += (
                self.current_stats.num_setups_updated
            )
            self.overall_stats.num_setups_triggered += (
                self.current_stats.num_setups_triggered
            )
            self.overall_stats.num_alerts_attempted += (
                self.current_stats.num_alerts_attempted
            )

        self.previous_stats.appendleft(self.current_stats)

        self._update_stats_post_run(exception)

    def flush_start_of_overall_run(self) -> None:
        overall_stats_dict = asdict(self.overall_stats)
        overall_stats_dict.pop("loop")
        self.overall_stats_db_instance: LiveLoopModel = LiveLoopModel.objects.create(
            **overall_stats_dict
        )

        self._flush_start_of_overall_run()

        self.overall_stats_db_instance.save()

    def flush_stats(self) -> None:
        for overall_key, overall_value in asdict(self.overall_stats).items():
            if overall_key == "loop":
                continue
            setattr(self.overall_stats_db_instance, overall_key, overall_value)

        current_flush_stats_db_instances: list[LiveLoopRunModel] = []
        for loop_run_stats in reversed(self.previous_stats):
            instance: LiveLoopRunModel = LiveLoopRunModel()
            for run_key, run_value in asdict(loop_run_stats).items():
                if run_key == "loop":
                    run_value = self.overall_stats_db_instance
                setattr(instance, run_key, run_value)
            current_flush_stats_db_instances.append(instance)
        self.current_flush_stats_db_instances: list[
            LiveLoopRunModel
        ] = current_flush_stats_db_instances

        self._flush_stats()

        flush_datetime = timezone.now()
        self.overall_stats.last_stats_flush_at = flush_datetime
        self.overall_stats_db_instance.last_stats_flush_at = flush_datetime
        for run_stats in self.current_flush_stats_db_instances:
            run_stats.flushed_at = flush_datetime

        self.overall_stats_db_instance.save()
        if self.current_flush_stats_db_instances:
            LiveLoopRunModel.objects.bulk_create(
                self.current_flush_stats_db_instances, batch_size=500
            )

        self.current_flush_stats_db_instances = []
        self.previous_stats.clear()

    def get_overall_stats_class_init_defaults(self) -> dict[str, Any]:
        return {
            "loop": self,
            "start_datetime": self.start_datetime,
            "start_perf": self.start_perf,
            "last_datetime": self.start_datetime,
            "last_perf": self.start_perf,
            "last_stats_flush_at": self.start_datetime,
        }

    def get_current_stats_class_init_defaults(self) -> dict[str, Any]:
        return {
            "loop": self,
            "start_datetime": self.current_datetime,
            "start_perf": self.current_perf,
            "end_datetime": self.current_datetime,
            "end_perf": self.current_perf,
            "run_number": self.overall_stats.last_run_number + 1,
        }

    def run(self) -> None:
        self.start_datetime = timezone.now()
        self.start_perf = perf_counter()
        self.current_datetime = self.start_datetime
        self.current_perf = self.start_perf

        self.overall_stats: OverallLoopRunStatistics = self.overall_stats_class(
            **self.get_overall_stats_class_init_defaults()
        )
        self.current_stats: OneLoopRunStatistics = self.current_stats_class(
            **self.get_current_stats_class_init_defaults()
        )
        self.previous_stats: deque[OneLoopRunStatistics] = deque([])

        # Create the overall stats record in the database to represent this run.
        self.flush_start_of_overall_run()

        while True:
            self.current_datetime = timezone.now()
            self.current_perf = perf_counter()
            self.current_stats = self.current_stats_class(
                **self.get_current_stats_class_init_defaults()
            )

            try:
                with transaction.atomic():
                    self.run_next_iteration()
            except Exception as e:
                if isinstance(e, LoopRunExit):
                    self.handle_loop_run_exit(e)
                    self.logger.info(
                        (
                            "Ran into a loop run exit during a loop run "
                            "(reason=%s, detail=%s)."
                        ),
                        (e.reason or ""),
                        (e.detail or ""),
                    )
                else:
                    self.logger.exception(
                        (
                            "Ran into unhandled exception during a loop run "
                            "(symbol_type=%s)."
                        ),
                        self.symbol_type,
                    )
                    self.handle_loop_run_unhandled_exception(e)
                self.update_stats_post_run(e)
            else:
                self.update_stats_post_run(None)

            # Check and flush stats records.
            end_datetime = timezone.now()
            if (
                end_datetime - self.overall_stats.last_stats_flush_at
                > self.min_stats_record_duration
            ):
                pretty_stats_elapsed = str(
                    Decimal(
                        (
                            end_datetime - self.overall_stats.last_stats_flush_at
                        ).total_seconds()
                    ).quantize(Decimal("0.0001"))
                )
                self.logger.info(
                    "Flushing stats given that %s second(s) have elapsed.",
                    pretty_stats_elapsed,
                )
                self.flush_stats()
                end_datetime = timezone.now()

            # End of loop. Decide if we want/need to sleep or not.
            end_perf = perf_counter()
            end_diff = end_perf - self.current_perf
            min_wait_duration = self.min_wait_duration
            pretty_end_diff = str(Decimal(end_diff).quantize(Decimal("0.0001")))
            if end_diff < min_wait_duration.total_seconds():
                sleep_for = min_wait_duration.total_seconds() - end_diff
                pretty_sleep_for = str(Decimal(sleep_for).quantize(Decimal("0.0001")))
                self.logger.info(
                    "Finished loop run in %s second(s). Sleeping for %s second(s).",
                    pretty_end_diff,
                    pretty_sleep_for,
                )
                time.sleep(sleep_for)
            else:
                self.logger.info(
                    "Finished loop run in %s second(s). Not sleeping.", pretty_end_diff
                )

    # --- Private Implementations that are 100% delegated to subclasses. ---
    #
    # All of these methods below are called by the corresponding method with the same
    # name except that the corresponding name doesn't start with _. They allow
    # subclasses to define them and override them to add subclass-specific
    # functionality.

    def _flush_start_of_overall_run(self) -> None:
        ...

    def _handle_loop_run_unhandled_exception(self, exception: Exception) -> None:
        ...

    def _handle_loop_run_exit(self, exit: LoopRunExit) -> None:
        ...

    def _update_stats_pre_run(self) -> None:
        ...

    def _run_pre_run_checks(self) -> None:
        ...

    def _check_and_refresh_setups(self) -> None:
        ...

    def _refresh_latest_prices(self) -> None:
        ...

    def _run_next_iteration(self) -> None:
        ...

    def _check_timeframe_before_checking_setups(self, timeframe: Timeframe) -> None:
        ...

    def _check_setup(self, symbol: SymbolRec, setup: Setup) -> None:
        ...

    def _check_and_persist_updated_setups(self) -> None:
        ...

    def _queue_prepared_alerts(self) -> None:
        ...

    def _update_stats_post_run(self, exception: Exception | None) -> None:
        ...

    def _flush_stats(self) -> None:
        ...

    # ---                                                                ---
