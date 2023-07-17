# %%
import os
from uuid import uuid4
from dotenv import load_dotenv, find_dotenv

from confluent_kafka import Consumer
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer

# %%
_ = load_dotenv(find_dotenv())

# %%
sr_conf = {
    "url": os.environ["SCHEMA_REGISTRY_URL"],
    "basic.auth.user.info": f"{os.environ['SCHEMA_REGISTRY_API_KEY']}:{os.environ['SCHEMA_REGISTRY_API_SECRET']}",
}
schema = "order_info.avsc"
path = os.path.realpath(os.path.dirname(__file__))
with open(f"{path}/avro/{schema}") as f:
    schema_str = f.read()
schema_registry_client = SchemaRegistryClient(sr_conf)
# %%
topic = "topic_0"
avro_deserializer = AvroDeserializer(schema_registry_client, schema_str)

consumer_conf = {
    "bootstrap.servers": os.environ["BOOTSTRAP_SERVERS"],
    "group.id": f"avro_consumer_{uuid4()}",
    "auto.offset.reset": "earliest",
    "security.protocol": "SASL_SSL",
    "sasl.mechanism": "PLAIN",
    "sasl.username": os.environ["SASL_USERNAME"],
    "sasl.password": os.environ["SASL_PASSWORD"],
    "client.id": "avro_producer",
}
# %%
consumer = Consumer(consumer_conf)
consumer.subscribe([topic])
# %%
while True:
    try:
        # SIGINT can't be handled when polling, limit timeout to 1 second.
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        
        # print(msg.value())
        user = avro_deserializer(
            msg.value(), SerializationContext(msg.topic(), MessageField.VALUE)
        )
        if user is not None:
            print(user)
    except KeyboardInterrupt:
        break

consumer.close()

# %%
