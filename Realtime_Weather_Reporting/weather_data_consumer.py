from kafka import KafkaConsumer
from pprint import pprint
import json

def kafka_consumer(topic_name, ip):
    consumer = KafkaConsumer(topic_name,
                            auto_offset_reset='earliest',
                            bootstrap_servers=[ip],
                            api_version=(0, 10, 0),
                            value_deserializer = json.loads,
                            consumer_timeout_ms=1000)

    for msg in consumer:
        pprint(msg.value)

kafka_consumer('weather-stream', 'localhost')