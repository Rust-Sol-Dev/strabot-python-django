from confluent_kafka import Producer
from faker import Faker
import json
from orjson import orjson
import logging
import random
from time import perf_counter


fake = Faker()

logging.basicConfig(format='%(asctime)s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    filename='producer.log',
                    filemode='w')

logger = logging.getLogger()
logger.setLevel(logging.INFO)

####################
conf = {
    'bootstrap.servers': 'redpanda-01.stratalerts.com:9093',
    'security.protocol': 'SASL_PLAINTEXT',
    'sasl.mechanism': 'SCRAM-SHA-256',
    # 'sasl.username': 'admin',
    # 'sasl.password': 'KZ6trrE3j8gLsoBQiBQKdqL5DcQh6PWQ',
    'sasl.username': 'quix',
    'sasl.password': 'cJCz9SeoXev9HF9D626TxoNGG2vNuWvD',
    'group.id': 'demo-consumer',
    'enable.auto.commit': True,

}
p = Producer(**conf)

print('Kafka Producer has been initiated...')


#####################
def receipt(err, msg):
    if err is not None:
        print('Error: {}'.format(err))
    else:
        message = 'Produced message on topic {} with value of {}\n'.format(msg.topic(), msg.value().decode('utf-8'))
        logger.info(message)


def main():
    msgs = 0
    s = perf_counter()
    for i in range(100000):
        data = {
            'user_id': fake.random_int(min=20000, max=100000),
            'user_name': fake.name(),
            'user_address': fake.street_address() + ' | ' + fake.city() + ' | ' + fake.country_code(),
            'platform': random.choice(['Mobile', 'Laptop', 'Tablet']),
            'signup_at': str(fake.date_time_this_month())
        }
        # m = orjson.dumps(data)
        m = orjson.dumps({'a': 1})
        msgs += 1
        # p.poll(1)
        p.produce('demo', m, callback=receipt)
        # time.sleep(0.1)
    p.flush()
    elapsed = perf_counter() - s
    print(f'completed in {elapsed * 1000:.4f} ms ({msgs / elapsed:.2f} msgs/sec {elapsed:.2f} s)')


if __name__ == '__main__':
    main()
