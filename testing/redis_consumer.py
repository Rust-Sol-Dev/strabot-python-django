"""
Start processing only latest records:
$ python consumer.py consumer1 --start-from $
Start processing all records in the stream from the beginning:
$ python consumer.py consumer1 --start-from 0
"""
from collections import defaultdict
from datetime import datetime, timedelta
from time import perf_counter

import pandas as pd
import typer
import random
import time
from walrus import Database
from enum import Enum


LAST_ID_KEY = "{consumer_id}:lastid"
BLOCK_TIME = 0
STREAM_KEY = "crypto:binance"

app = typer.Typer()


class StartFrom(str, Enum):
    beginning = "0"
    latest = "$"


pricerecs = defaultdict(pd.DataFrame)


@app.command()
def start(consumer_id: str, start_from: StartFrom = StartFrom.beginning):
    rdb = Database()
    stream = rdb.Stream(STREAM_KEY)

    last_id = rdb.get(LAST_ID_KEY.format(consumer_id=consumer_id))

    last_id = None
    if last_id:
        print(f"Resume from ID: {last_id}")
    else:
        last_id = start_from.value
        print(f"Starting from {start_from.name}")

    while True:
        print("Reading stream...")
        # messages = stream.read(last_id=last_id, block=BLOCK_TIME)
        messages = stream.read(last_id=last_id, block=BLOCK_TIME)
        s = perf_counter()
        if messages:
            for message_id, message in messages:
                print(f"processing {message_id}::{message}")
                last_id = message_id
                rdb.set(LAST_ID_KEY.format(consumer_id=consumer_id), last_id)
                print(f"finished processing {message_id} at {datetime.utcnow()}")
                message_dict = {k.decode('utf-8'): v.decode('utf-8') for k, v in message.items()}
                symbol = message_dict['symbol']
                df = pricerecs[symbol]
                new_df = pd.DataFrame(data=message_dict, index=pd.to_datetime([int(message_dict['timestamp'])], unit='ms', utc=True))
                overlap = df.index.isin(new_df.index)
                if overlap.any():
                    df = pd.concat([df, new_df])
                    df = df[~df.index.duplicated(keep='last')]
                else:
                    df = pd.concat([df, new_df])
                cutoff_date = datetime.utcnow().astimezone() - timedelta(days=7)
                mask = df.index >= cutoff_date
                df = df.loc[mask]
                pricerecs[symbol] = df
                print('===============================================================')
                print(df.tail())
        else:
            print(f"No new messages after ID: {last_id}")
        elapsed = perf_counter() - s
        print(f'completed in {elapsed * 1000:.4f} ms ({elapsed:.2f} s)')


if __name__ == "__main__":
    app()
