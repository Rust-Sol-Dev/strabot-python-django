import math
from datetime import datetime, timedelta
import os

from quixstreams import Application, State, message_key


os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings.dev")
import django
django.setup()
from django.conf import settings


kafka_conf = {
    'security.protocol': settings.REDPANDA_SECURITY_PROTOCOL,
    'sasl.mechanism': settings.REDPANDA_SASL_MECHANISM,
    'sasl.username': settings.REDPANDA_USERNAME,
    'sasl.password': settings.REDPANDA_PASSWORD
}

# Define an application
app = Application(
    broker_address="redpanda-01.stratalerts.com:9093",  # Kafka broker address
    consumer_group="quix-group",  # Kafka consumer group
    consumer_extra_config=kafka_conf,  # Kafka consumer configuration
    producer_extra_config=kafka_conf,  # Kafka producer configuration
    state_dir=".state",
)

# Create input and output topics for ticker data
# Assume that messages in the topics are in JSON format
# and the ticker symbols are used as message keys.
input_topic = app.topic("BINANCE.trade", value_deserializer="json")
output_topic = app.topic("tickers-downsampled", value_serializer="json")


# Create a StreamingDataFrame and connect it to the input topic
# StreamingDataFrame is a declarative interface.
# Use it to define your transformations.
# After it's done, pass it to `app.run()` and it will start consuming data from Kafka
# and processing it.
sdf = app.dataframe(topic=input_topic)


def _floor_datetime(dt: datetime, delta: timedelta) -> datetime:
    """
    Round datetime down given the timedelta
    :param dt: datetime object
    :param delta: timedelta object
    :return: rounded datetime object

    Example:
    > floor_datetime(dt=datetime(2023, 22, 11, 17, 58, 23), delta=timedelta(minutes=1))
    datetime(2023, 22, 11, 17, 58, 0)
    """
    return datetime.min + math.floor((dt - datetime.min) / delta) * delta


def downsample_tickers(ticker: dict, state: State):
    """
    Define a function to downsample tickers data to 1m, 15m and 60m frequency
    and return the most recent aggregation to produce downstream.

    It accepts the current deserialized message ("ticker")
    and a State instance to save data to RocksDB ("state").

    The assumed format of the ticker data:
     {
       "timestamp": 123.123 - time in seconds since epoch (UTC)
       "price": 123 - current price of the ticker
     }

    :param ticker: ticker value from the topic
    :param state: instance of `State` to access the underlying RocksDB store.
    :return: downsample ticker data in the format {"<time bucket>": {"first": float, "last": float, "min": float, "max": float}}
    """

    # Convert epoch timestamp to datetime
    dt = datetime.fromtimestamp(ticker["T"] / 1000.0)

    # Create a list of buckets to do the aggregation
    time_buckets = [
        # _floor_datetime(dt, delta=timedelta(minutes=1)),
        _floor_datetime(dt, delta=timedelta(minutes=15)),
        # _floor_datetime(dt, delta=timedelta(minutes=30)),
        # _floor_datetime(dt, delta=timedelta(hours=4)),
        # _floor_datetime(dt, delta=timedelta(hours=6)),
        # _floor_datetime(dt, delta=timedelta(hours=12)),
        # _floor_datetime(dt, delta=timedelta(days=1)),
    ]

    # Define a dict to store all aggregations and produc
    result = {}
    for bucket in time_buckets:
        new_aggregate = {}
        # Convert bucket to iso to use as a state key
        # The State object already uses the current Kafka key as a prefix
        # (ticker symbol in our case), so data for different message keys will not
        # overwrite each other.
        bucket_iso = bucket.isoformat()
        # Get a previous aggregation if it exists
        latest_aggregate = state.get(bucket_iso, default={})

        price = float(ticker['p'])

        # Set a new "first" if it doesn't exist
        if not latest_aggregate.get("open"):
            new_aggregate["open"] = price
        else:
            new_aggregate["open"] = latest_aggregate["open"]
        # Calculate "last"
        new_aggregate["close"] = price

        # Set new "min" and "max"
        new_aggregate["low"] = min(price, latest_aggregate.get("low", float('inf')))
        new_aggregate["high"] = max(price, latest_aggregate.get("high", float('-inf')))

        # Save new aggregate to the state
        state.set(bucket_iso, new_aggregate)
        # Add new aggregate to result
        result[bucket_iso] = new_aggregate

    # Return a dict with aggregates to produce downstream
    return result


sdf = (
    sdf
    # Schedule the downsampling function to be executed on streaming data
    .apply(downsample_tickers, stateful=True)
    # Print the value before sending it
    .update(
        lambda value: print(
            f'Ticker: [{message_key().decode()}]', "Aggregation: ", value
        )
    )
    # Send the latest aggregates to the output topic with the same message keys
    # .to_topic(output_topic)
)

if __name__ == "__main__":
    # Run the streaming application
    app.run(sdf)
