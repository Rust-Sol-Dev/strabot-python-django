from datetime import datetime, timezone
from time import perf_counter

import psycopg2
from bytewax.outputs import StatelessSinkPartition, DynamicSink
from orjson import orjson
from psycopg2.extras import execute_values


class PostgresqlRawSinkPartition(StatelessSinkPartition):

    def __init__(self):
        self._client = psycopg2.connect(
            'postgresql://stratbot_pg_user:jyufwZbuNYaq78RCECpXCJUHoETuhpXB@159.223.176.230:5432/stratbot'
            # 'postgresql://stratbot_pg_user:jyufwZbuNYaq78RCECpXCJUHoETuhpXB@10.116.0.7:5432/stratbot'
        )

    def write_batch(self, items: list) -> None:
        insert_sql = """
            INSERT INTO staging_ohlcv (symbol, tf, ohlcv) VALUES %s
            ON CONFLICT (symbol, tf)
            DO UPDATE SET ohlcv = EXCLUDED.ohlcv
        """

        data = []
        for symbol, values in items:
            for tf, bars in values.items():
                data.append((
                    symbol,
                    tf,
                    # datetime.fromtimestamp(bar['ts'], tz=timezone.utc),
                    orjson.dumps(bars).decode('utf-8')
                ))

        try:
            with self._client.cursor() as cur:
                execute_values(cur, insert_sql, data)
                self._client.commit()
        except (psycopg2.DatabaseError, psycopg2.OperationalError) as e:
            print("error while inserting data", e)
            self._client.rollback()


class PostgresqlRawSink(DynamicSink):
    def build(self, worker_index, worker_count) -> PostgresqlRawSinkPartition:
        return PostgresqlRawSinkPartition()


# ======================================================================================================================


class PsqlTradePartition(StatelessSinkPartition):

    def __init__(self):
        self._client = psycopg2.connect(
            'postgresql://stratbot_pg_user:jyufwZbuNYaq78RCECpXCJUHoETuhpXB@159.223.176.230:5432/stratbot'
            # 'postgresql://stratbot_pg_user:jyufwZbuNYaq78RCECpXCJUHoETuhpXB@10.116.0.7:5432/stratbot'
        )

    def write_batch(self, items: list) -> None:
        insert_sql = """
            INSERT INTO crypto_trades (
                event_type,
                event_time,
                agg_trade_id,
                symbol,
                price,
                quantity,
                first_trade_id,
                last_trade_id,
                time,
                is_market_maker
            ) VALUES %s
        """

        data = []
        for symbol, trade in items:
            # value = {'e': 'aggTrade', 'E': 1702340143083, 'a': 164706577, 's': 'OPUSDT', 'p': '2.2723000', 'q': '643.4', 'f': 422191204, 'l': 422191206, 'T': 1702340143082, 'm': False}
            print(symbol, trade)
            trade['p'] = float(trade['p'])
            trade['q'] = float(trade['q'])
            trade['T'] = datetime.fromtimestamp(trade['T'] / 1000.0, tz=timezone.utc)
            data.append(tuple(trade.values()))

        try:
            with self._client.cursor() as cur:
                execute_values(cur, insert_sql, data)
                self._client.commit()
        except (psycopg2.DatabaseError, psycopg2.OperationalError) as e:
            print("error while inserting data", e)
            self._client.rollback()


class PsqlTradeSink(DynamicSink):
    def build(self, worker_index, worker_count) -> PsqlTradePartition:
        return PsqlTradePartition()

# ======================================================================================================================


class PostgresqlSetupSinkPartition(StatelessSinkPartition):

    def write_batch(self, items: list) -> None:
        for symbol, setup in items:
            s = perf_counter()
            print(symbol, setup)
            setup.save()
            elapsed = perf_counter() - s
            print(f'completed in {elapsed * 1000:.4f} ms ({elapsed:.2f} s)')


class PostgresqlSetupSink(DynamicSink):
    def build(self, step_id: str, worker_index, worker_count) -> PostgresqlSetupSinkPartition:
        return PostgresqlSetupSinkPartition()
