from confluent_kafka import Producer
from config import read_ccloud_config

producer = Producer(read_ccloud_config("client.properties"))
producer.produce("my-topic", key="key", value="value")