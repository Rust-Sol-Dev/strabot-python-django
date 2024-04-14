import redis

from bytewax.outputs import StatelessSinkPartition, DynamicSink
import msgspec
from orjson import orjson


class RedisSinkPartition(StatelessSinkPartition):
    """
    Output partition that writes to a Redis instance.
    """
    def __init__(self, client: redis.Redis, key_prefix: str = ''):
        self.client = client
        self.key_prefix = f'{key_prefix}' if key_prefix else ''

    def write_batch(self, batch: list) -> None:
        """
        Write a batch of output items to Redis. Called multiple times whenever new items are seen at this
        point in the dataflow. Non-deterministically batched.
        """
        with self.client.pipeline() as pipe:
            for key, value in batch:
                pipe.json().set(f'{self.key_prefix}{key}', '$', value)
            pipe.execute()

    def close(self) -> None:
        """
        Cleanup this partition when the dataflow completes. This is not guaranteed to be called on unbounded data.
        """
        self.client.close()


class RedisSink(DynamicSink):
    """
    An output sink where all workers write items to a Redis instance concurrently.

    Does not support storing any resume state. Thus, these kind of
    outputs only naively can support at-least-once processing.
    """
    def __init__(self, client: redis.client.Redis, key_prefix: str = ''):
        self.client = client
        self.key_prefix = key_prefix

    def build(self, step_id: str, worker_index: int, worker_count: int) -> RedisSinkPartition:
        """
        Build an output partition for a worker. Will be called once on each worker.
        """
        return RedisSinkPartition(self.client, self.key_prefix)
