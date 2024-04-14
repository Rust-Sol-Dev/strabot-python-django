from collections import defaultdict
from datetime import datetime, timezone

import pytz
from bytewax.inputs import StatelessSourcePartition, DynamicSource
from django.db.models import QuerySet

from stratbot.scanner.models.symbols import Setup


class PostgresqlSourcePartition(StatelessSourcePartition):

    def __init__(self, query: QuerySet):
        self.setups = query
        # self.setups = (
        #     Setup.objects
        #     .prefetch_related('symbol_rec')
        #     .filter(expires__gt=datetime.now(tz=pytz.utc))
        #     .filter(symbol_rec__symbol_type='crypto')
        #     .filter(hit_magnitude=False)
        # )

    def next_batch(self, sched: datetime):
        setups_by_symbol = defaultdict(list)

        for setup in self.setups:
            setups_by_symbol[setup.symbol_rec.symbol].append(setup)

        return [(symbol, setups) for symbol, setups in setups_by_symbol.items()]

    def next_awake(self):
        pass

    def close(self):
        pass


class PostgresqlSource(DynamicSource):
    def __init__(self, query: QuerySet):
        self.query = query

    def build(self, now: datetime, worker_index: int, worker_count: int) -> PostgresqlSourcePartition:
        return PostgresqlSourcePartition(self.query)


# class PostgresqlSourcePartition(StatelessSourcePartition):
#
#     def next_batch(self, sched: datetime):
#         setups_by_symbol = defaultdict(list)
#
#         setups = (Setup.objects
#                   .prefetch_related('symbol_rec')
#                   .filter(expires__gt=datetime.now(tz=pytz.utc))
#                   .filter(symbol_rec__symbol_type='stock')
#                   .filter(hit_magnitude=False)
#                   )
#
#         for setup in setups:
#             setups_by_symbol[setup.symbol_rec.symbol].append(setup)
#
#         return [(symbol, setups) for symbol, setups in setups_by_symbol.items()]
#
#     def next_awake(self) -> Optional[datetime]:
#         pass
#
#     def close(self):
#         pass
#
#
# class PostgresqlSource(DynamicSource):
#
#     def build(
#             self, now: datetime, worker_index: int, worker_count: int
#     ) -> PostgresqlSourcePartition:
#         return PostgresqlSourcePartition()
