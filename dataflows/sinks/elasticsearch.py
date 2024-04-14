from datetime import datetime

from bytewax.outputs import StatelessSinkPartition, DynamicSink
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk


class ElasticSinkPartition(StatelessSinkPartition):
    """
    Output partition that writes to an Elasticsearch instance.
    """

    def __init__(self):
        self._client = Elasticsearch(
            cloud_id='STRATBOT:dXMtZWFzdDQuZ2NwLmVsYXN0aWMtY2xvdWQuY29tOjQ0MyQ4ZTg2ZjIxY2NhYTA0Y2JmYmIyNjA0NDYzZWNkYWMwZSQ2MTA2YTFiZWY0MjY0OWNhOTJiNGVlOTIzZGI0MWM5ZA==',
            http_auth=('elastic', 'sKODpHYHpp3eEmir6CnBiK3F'),
        )

    def write_batch(self, items: list) -> None:
        documents = []
        for symbol, tf__bars in items:
            for tf, bars in tf__bars.items():
                for bar in bars:
                    ts = datetime.fromtimestamp(bar['ts'])
                    doc_id = f"{symbol}_{tf}_{ts}"
                    # Add the action-and-metadata pair
                    documents.append({"index": {"_index": "crypto", "_id": doc_id}})
                    # Add the source document
                    documents.append({
                        "symbol": symbol,
                        "timeframe": tf,
                        "bar": bar,
                        "_timestamp": ts
                    })
        self._client.bulk(operations=documents, pipeline="ent-search-generic-ingestion")

    def close(self) -> None:
        pass


class ElasticSink(DynamicSink):
    """
    An output sink where all workers write items to an Elasticsearch instance concurrently.

    Does not support storing any resume state. Thus, this kind of
    outputs only naively can support at-least-once processing.
    """
    def build(self, worker_index: int, worker_count: int) -> ElasticSinkPartition:
        """
        Build an output partition for a worker. Will be called once on each worker.
        """
        return ElasticSinkPartition()
