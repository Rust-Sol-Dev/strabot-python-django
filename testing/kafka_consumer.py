import time
import json
from datetime import datetime

from confluent_kafka import Consumer

conf = {
    'bootstrap.servers': 'redpanda-01.stratalerts.com:9093',
    'security.protocol': 'SASL_PLAINTEXT',
    'sasl.mechanism': 'SCRAM-SHA-256',
    # 'sasl.username': 'admin',
    # 'sasl.password': 'KZ6trrE3j8gLsoBQiBQKdqL5DcQh6PWQ',
    # 'sasl.username': 'tinybird',
    # 'sasl.password': 'D5rsBizC89raiPXPK8J7v6NKfqTm5r4M',
    'sasl.username': 'timeplus',
    'sasl.password': 'arS0bE7wRICxyw]+WrzInlHO/_S9Z4',
    'group.id': 'test-consumer',
    'auto.offset.reset': 'earliest'
}
c = Consumer(**conf)

print('Kafka Consumer has been initiated...')
print('Available topics to consume: ', c.list_topics().topics)
# c.subscribe(['BINANCE.quotes'])
# c.subscribe(['^BINANCE\..*'])
# c.subscribe(['^POLYGON\..*\.T$'])
# c.subscribe(['BINANCE.24hrTicker'])
c.subscribe(['ALPACA'])


def main():
    message_count = 0
    start_time = time.time()

    while True:
        msg = c.poll(0.001)  # timeout
        if msg is None:
            continue
        if msg.error():
            print('Error: {}'.format(msg.error()))
            continue

        message_count += 1
        elapsed_time = time.time() - start_time
        if elapsed_time >= 1:
            print(f'Messages per second: {message_count / elapsed_time}')
            message_count = 0
            start_time = time.time()

        data = msg.value().decode('utf-8')
        data = json.loads(data)
        timestamp = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%fZ')
        print(timestamp, data)
    c.close()


if __name__ == '__main__':
    main()
