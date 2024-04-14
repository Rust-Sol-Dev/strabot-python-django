from collections import defaultdict
from functools import cached_property
import multiprocessing as mp
from datetime import datetime
import logging
import pytz

# from arcticdb import Arctic
from django.db.models import QuerySet
from django.conf import settings
from django.utils import timezone

from stratbot.scanner.models.exchange_calendar import ExchangeCalendar
from stratbot.scanner.models.timeframes import Timeframe
from v1.alerts.console import ConsoleStockAlert, ConsoleAlert
from v1.perf import func_timer
from stratbot.scanner.models.symbols import SymbolRec, Setup, SymbolType
from stratbot.scanner.ops import historical


# ac = Arctic(settings.ARCTIC_DB_URI)


class ScanEngine:
    def __init__(self):
        self.symbolrecs = {}
        self.pricerecs = defaultdict(dict)
        self.last_refresh = None
        self.started_on = datetime.now(tz=pytz.utc)
        self.engine_type = None
        self.library = None
        self.timeframes = list[Timeframe]

    def load_symbolrecs(self):
        self.symbolrecs = {rec.symbol: rec for rec in SymbolRec.objects.filter(symbol_type=self.engine_type)}

    def load_pricerecs(self):
        for symbol, symbolrec in self.symbolrecs.items():
            for tf in symbolrec.scan_timeframes:
                self.pricerecs[symbolrec.symbol][tf] = getattr(symbolrec, symbolrec.TF_MAP[tf])

    def load_pricerecs_for_arctic(self):
        for symbol, symbolrec in self.symbolrecs.items():
            for tf in ['1', 'D']:
                self.pricerecs[symbolrec.symbol][tf] = getattr(symbolrec, symbolrec.TF_MAP[tf])

    @property
    def symbols(self) -> list:
        return list(self.symbolrecs.keys())

    @property
    def setups(self) -> QuerySet:
        return (Setup.objects
                .prefetch_related('symbol_rec')
                .filter(expires__gt=timezone.now())
                .filter(symbol_rec__symbol_type=self.engine_type)
                # .filter(hit_magnitude=False)
                .filter(negated=False)
                .filter(tf__in=self.timeframes)
                )

    @cached_property
    def setups_by_direction(self):
        ordered_setups = defaultdict(lambda: defaultdict(list))
        for setup in self.setups:
            ordered_setups[setup.symbol_rec.symbol][setup.direction].append(setup)
        return ordered_setups

    def apply_strategy(self):
        pass

    @staticmethod
    def _scan(symbolrec, timeframes: list, rr: float):
        return symbolrec.symbol, symbolrec.scan_strat_setups(timeframes=timeframes, rr=rr)

    @func_timer
    def queue_scan(self, timeframes: list = None, rr: float = 0.1):
        results = []
        with mp.Pool() as pool:
            for symbol in self.symbols:
                results.append(pool.apply_async(self._scan, args=(self.symbolrecs[symbol], timeframes, rr)))
            current_setups = [result.get() for result in results]
        for symbol, setups in current_setups:
            for tf in setups.keys():
                if self.symbolrecs[symbol].setups.get(tf):
                    logging.info(f'setup exists: {symbol} [{tf}]')
                else:
                    for setup in setups[tf]:
                        logging.info(f'adding: {symbol}, {setup}')
                    self.symbolrecs[symbol].setups[tf] = setups[tf]

    def __str__(self):
        return self.engine_type


class CryptoEngine(ScanEngine):
    def __init__(self, timeframes: list[Timeframe]):
        super().__init__()
        self.engine_type = SymbolType.CRYPTO
        # self.library = ac.get_library(self.engine_type, create_if_missing=True)
        self.console = ConsoleAlert()
        self.load_symbolrecs()
        self.timeframes = timeframes


class StockEngine(ScanEngine):
    def __init__(self, timeframes: list[Timeframe]):
        super().__init__()
        self.engine_type = SymbolType.STOCK
        # self.library = ac.get_library(self.engine_type, create_if_missing=True)
        self.console = ConsoleStockAlert()
        self.exchange_calendar = ExchangeCalendar()
        self.load_symbolrecs()
        self.timeframes = timeframes

