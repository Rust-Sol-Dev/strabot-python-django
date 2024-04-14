# ======================================================================================================================
# CRYPTO
# ======================================================================================================================

from deephaven.stream.kafka import consumer as kc
from deephaven import dtypes as dht
from deephaven import time as dhtu
from deephaven import agg
from deephaven.plot.figure import Figure
from deephaven.updateby import rolling_group_time
from deephaven import merge


kafka_conf = {
    "bootstrap.servers": "redpanda-01.stratalerts.com:9093",
    "security.protocol": "SASL_PLAINTEXT",
    "sasl.mechanism": "SCRAM-SHA-256",
}

crypto = kc.consume(
    kafka_conf,
    "BINANCE.aggTrade",
    offsets=kc.ALL_PARTITIONS_SEEK_TO_BEGINNING,
    table_type=kc.TableType.ring(500_000),
    key_spec=kc.simple_spec("Symbol", dht.string),
    value_spec=kc.json_spec(
        [
            ("Timestamp", dht.Instant),
            ("Price", dht.double),
            ("Quantity", dht.double),
        ],
        mapping={
            "T": "Timestamp",
            "p": "Price",
            "q": "Quantity",
        }
    ),
)

crypto_bin = crypto.update(
    [
        "Volume_USD = Price * Quantity",
        "Fifteen = lowerBin(Timestamp, 15 * MINUTE)",
        "Thirty = lowerBin(Timestamp, 30 * MINUTE)",
        "Hourly = lowerBin(Timestamp, 1 * HOUR)",
        "Daily = lowerBin(Timestamp, 1 * DAY)",
        "Weekly = lowerBin(Timestamp, 1 * WEEK, 4 * DAY)",
    ]
)

agg_list = [
    agg.first(cols=["Open = Price"]),
    agg.max_(cols=["High = Price"]),
    agg.min_(cols=["Low = Price"]),
    agg.last(cols=["Close = Price"]),
    agg.sum_(cols=["Volume = Quantity"]),
    agg.sum_(cols=["Volume_USD = Volume_USD"]),
]

crypto_bin_sorted = crypto_bin.sort(order_by=["Fifteen"])
ohlcv_15 = crypto_bin_sorted.agg_by(agg_list, by=["Symbol", "Fifteen"]) #.last_by(by=["Symbol"])
ohlcv_30 = crypto_bin_sorted.agg_by(agg_list, by=["Symbol", "Thirty"]) #.last_by(by=["Symbol"])
ohlcv_60 = crypto_bin_sorted.agg_by(agg_list, by=["Symbol", "Hourly"]) #.last_by(by=["Symbol"])
ohlcv_d = crypto_bin_sorted.agg_by(agg_list, by=["Symbol", "Daily"]) #.last_by(by=["Symbol"])
ohlcv_w = crypto_bin_sorted.agg_by(agg_list, by=["Symbol", "Weekly"]) #.last_by(by=["Symbol"])


crypto_sum_by = crypto.view(formulas=["Symbol", "Trade_Count = 1", "Quantity", "Volume_USD = Price * Quantity"])\
    .sum_by(by=["Symbol"])\
    .sort_descending(order_by=["Volume_USD"])


rgt = rolling_group_time(ts_col="Fifteen", cols=["Prev_High = High", "Prev_Low = Low"], rev_time="PT00:15:00")
result = ohlcv_15.update_by(ops=[rgt], by="Symbol")


# crypto_bin = crypto.update(formulas=["TimestampBin = lowerBin(Timestamp, HOUR)"])

# agg_list = [
#     agg.first(cols=["OpenTime = Timestamp"]),
#     agg.max_(cols=["High = Price"]),
#     agg.min_(cols=["Low = Price"]),
#     agg.last(cols=["CloseTime = Timestamp"]),
# ]
#
# ohlc = crypto_bin.agg_by(agg_list, by=["Symbol", "TimestampBin"])\
#     .join(table=crypto_bin, on=["CloseTime = Timestamp"], joins=["Close = Price"])\
#     .join(table=crypto_bin,  on=["OpenTime = Timestamp"], joins=["Open = Price"])


# btc_bin = crypto.where(filters=["Symbol=`BTCUSDT`"]).update(formulas=["TimestampBin = lowerBin(Timestamp, 5 * SECOND)"])
#
# plot_ohlc = Figure()\
#     .figure_title("SPY OHLC")\
#     .plot_ohlc(series_name="SPY", t=t_ohlc, x="TimestampBin", open="Open", high="High", low="Low", close="Close")\
#     .show()
#
# plot_ohlc = Figure()\
#     .figure_title("BTC 5s")\
#     .plot(series_name="BTC", t=ohlcv_15, x="TimestampBin")\
#     .show()


desired_symbols = ["BTCUSDT", "ETHUSDT"]
crypto_filtered = crypto.where(
    ["Symbol in " + str(desired_symbols)]
)

crypto_stocks_aggregated = (
    crypto_filtered
    .view("Symbol", "Trade_Count = 1", "Quantity", "USD_Value = Price * Quantity")
    .sum_by("Symbol")
    .sort_descending("USD_Value")
)

# ======================================================================================================================
# STOCKS
# ======================================================================================================================

from deephaven.stream.kafka import consumer as kc
from deephaven import dtypes as dht
from deephaven import time as dhtu

kafka_conf = {
    "bootstrap.servers": "redpanda-01.stratalerts.com:9093",
    "security.protocol": "SASL_PLAINTEXT",
    "sasl.mechanism": "SCRAM-SHA-256",
}

stocks = kc.consume(
    kafka_conf,
    "ALPACA.trades",
    table_type=kc.TableType.append(),
    key_spec=kc.simple_spec("Symbol", dht.string),
    value_spec=kc.json_spec(
        [
            ("Timestamp", dht.Instant),
            ("Price", dht.double),
            ("Quantity", dht.double),
            ("Conditions", dht.string),
        ],
        mapping={
            "t": "Timestamp",
            "p": "Price",
            "s": "Quantity",
            "c": "Conditions",
        }
    ),
)