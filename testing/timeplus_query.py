import os
import traceback
import json
from pprint import pprint

from timeplus import Query, Environment

api_key = "z1CyIIaESJ2Qgv3dSMXU8_HcaSSMkpT-hRmimq1czhd4XBjYzNB9Qs9XAtDq"
api_address = "us.timeplus.cloud"
workspace = "caxg9ecv"

# Configure API key and address
env = Environment().address(api_address).workspace(workspace).apikey(api_key)

try:
    # list all qeuries
    query_list = Query(env=env).list()
    pprint(f"there are {len(query_list)} queries ")

    # create a new query
    # query = (
    #     Query(env=env).sql(query="select S, sum(s) as volume from table(alpaca_trades) where _tp_time >= to_datetime(to_start_of_day(now(), 'America/New_York') + 4h, 'America/New_York') group by S order by volume")
    ##     .batching_policy(1000, 1000)
        # .create()
    # )
    query = (
        Query(env=env).sql(query="SELECT * FROM table(coinbase_tickers_raw) limit 100")
        # .batching_policy(1000, 1000)
        .create()
    )

    pprint(f"query with metadata {json.dumps(query.metadata())}")

    # query header is the colume definitions of query result table
    # it is a list of name/value pair
    # for example : [{'name': 'in_use', 'type': 'bool'}, {'name': 'speed', 'type': 'float32'}]
    query_header = query.header()
    pprint(f"query with header {query.header()}")

    # iterate query result
    limit = 20
    count = 0

    # query.result() is an iterator which will pull all the query result in small batches
    # the iterator will continously pulling query result
    # for streaming query, the iterator will not end until user cancel the query
    for event in query.result():
        # metric event return result time query metrics
        # a sample metrics event:
        # {'count': 117, 'eps': 75, 'processing_time': 1560,
        # 'last_event_time': 1686237113265, 'response_time': 861,
        # 'scanned_rows': 117, 'scanned_bytes': 7605}
        if event.event == "metrics":
            pprint(json.loads(event.data))

        # message event contains query result which is an array of arrays
        # representing multiple query result rows
        # a sample message event:
        # [[True,-73.857],[False, 84.1]]
        if event.event == "message":
            pprint(json.loads(event.data))
        count += 1
        if count >= limit:
            break

    query.cancel()
    query.delete()

except Exception as e:
    pprint(e)
    traceback.print_exc()
