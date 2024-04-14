from bytewax.connectors.kafka import KafkaSinkMessage
# from orjson import orjson
import msgspec


def serialize(data):
    key, value = data
    value = msgspec.json.encode(value)
    return KafkaSinkMessage(key, value)


def deserialize(data):
    key = data.key
    key = key.decode('utf-8')

    value = data.value
    value = msgspec.json.decode(value)

    return key, value
