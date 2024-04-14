from datetime import datetime, timezone

from bytewax.outputs import StatelessSinkPartition, DynamicSink
from cassandra.cluster import Cluster


class ScyllaDBSinkPartition(StatelessSinkPartition):
    """
    CREATE TABLE crypto.ohlcv (
    symbol TEXT,
    tf TEXT,
    timestamp TIMESTAMP,
    o DOUBLE,
    h DOUBLE,
    l DOUBLE,
    c DOUBLE,
    v DOUBLE,
    sid TEXT,
    PRIMARY KEY ((symbol), tf, timestamp)
    ) WITH CLUSTERING ORDER BY (tf ASC, timestamp ASC);
    """
    def __init__(self):
        cluster = Cluster(['localhost'])
        self._session = cluster.connect('crypto')

    def write_batch(self, items: list) -> None:
        # CQL statement for inserting data, with upsert logic
        insert_cql = """
        INSERT INTO ohlcv (symbol, tf, timestamp, o, h, l, c, v, sid) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        # insert_cql = """
        # INSERT INTO ohlcv (symbol, tf, timestamp) VALUES (%s, %s, %s)
        # """

        data = []
        for symbol, values in items:
            for tf, item_list in values.items():
                item = item_list[-1]  # Get the last item for each timeframe
                ts = datetime.fromtimestamp(item['ts'], tz=timezone.utc)
                data.append((
                    symbol,
                    tf,
                    ts,
                    item['o'],
                    item['h'],
                    item['l'],
                    item['c'],
                    item['v'],
                    item['sid']
                ))

        try:
            for row in data:
                self._session.execute(insert_cql, row)
        except Exception as e:
            print("error while inserting data", e)


class ScyllaDBSink(DynamicSink):
    def build(self, worker_index, worker_count) -> ScyllaDBSinkPartition:
        return ScyllaDBSinkPartition()
