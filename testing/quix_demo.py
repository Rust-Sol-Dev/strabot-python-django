from quixstreams import Application, MessageContext, State


conf = {
    'security.protocol': 'SASL_PLAINTEXT',
    'sasl.mechanism': 'SCRAM-SHA-256',
    'sasl.username': 'admin',
    'sasl.password': 'KZ6trrE3j8gLsoBQiBQKdqL5DcQh6PWQ',
    # 'auto.offset.reset': 'latest',
    # 'group.id': 'test-consumer',
}

# Define an application
app = Application(
    broker_address="redpanda-01.stratalerts.com:9093",
    consumer_group="test-consumer",
    consumer_extra_config=conf,
)

# Define the input and output topics. By default, "json" serialization will be used
input_topic = app.topic("BINANCE.quotes")
# output_topic = app.topic("my_output_topic")


def add_one(data: dict, ctx: MessageContext):
    for field, value in data.items():
        if isinstance(value, int):
            data[field] += 1


def count(data: dict, ctx: MessageContext, state: State):
    # Get a value from state for the current Kafka message key
    total = state.get('total', default=0)
    total += 1
    # Set a value back to the state
    state.set('total', total)
    # Update your message data with a value from the state
    data['total'] = total


# Create a StreamingDataFrame instance
# StreamingDataFrame is a primary interface to define the message processing pipeline
sdf = app.dataframe(topic=input_topic)

# Print the incoming messages
sdf = sdf.apply(lambda value, ctx: print('Received a message:', value))

# Select fields from incoming messages
# sdf = sdf[["field_0", "field_2", "field_8"]]

# Filter only messages with "field_0" > 10 and "field_2" != "test"
# sdf = sdf[(sdf["field_0"] > 10) & (sdf["field_2"] != "test")]

# Apply custom function to transform the message
# sdf = sdf.apply(add_one)

# Apply a stateful function to persist data to the state store
# sdf = sdf.apply(count, stateful=True)

# Print the result before producing it
# sdf = sdf.apply(lambda value, ctx: print('Producing a message:', value))

# Produce the result to the output topic
# sdf = sdf.to_topic(output_topic)

if __name__ == "__main__":
    # Run the streaming application
    app.run(sdf)
