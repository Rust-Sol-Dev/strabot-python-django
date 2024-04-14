from bytewax.outputs import StatelessSinkPartition, DynamicSink
from pymongo import MongoClient, UpdateOne
from pymongo.server_api import ServerApi


class MongoSinkPartition(StatelessSinkPartition):
    """
    Output partition that writes to a MongoDB instance.
    """
    uri = "mongodb+srv://stratbot:yMU9uDQjWuETYbM9T4SB3mnDHvbNhQRq@serverlessinstance0.cyl6nmb.mongodb.net/?retryWrites=true&w=majority"

    def __init__(self):
        self._client = MongoClient(self.uri, server_api=ServerApi('1'))
        self._db = self._client["markets"]
        self._collection = self._db["crypto"]

    def write_batch(self, items: list) -> None:
        """
        Write a batch of output items to MongoDB. Each item in the list is a tuple of (symbol, bars_),
        where bars_ is a dictionary with timeframes as keys and lists of bar data as values.
        """
        update_operations = []

        for symbol, bars_ in items:
            for timeframe, bars in bars_.items():
                if bars:
                    last_bar = bars[-1]
                    update_query = {"symbol": symbol}
                    update_data = {"$set": {f"bars.{timeframe}": last_bar}}
                    update_operations.append(UpdateOne(update_query, update_data, upsert=True))

        if update_operations:
            self._collection.bulk_write(update_operations)

    def close(self) -> None:
        """
        Cleanup this partition when the dataflow completes. This is not guaranteed to be called on unbounded data.
        """
        self._client.close()


class MongoSink(DynamicSink):
    """
    An output sink where all workers write items to a MongoDB instance concurrently.

    Does not support storing any resume state. Thus, these kind of
    outputs only naively can support at-least-once processing.
    """
    def build(self, step_id: str, worker_index: int, worker_count: int) -> MongoSinkPartition:
        """
        Build an output partition for a worker. Will be called once on each worker.
        """
        return MongoSinkPartition()
