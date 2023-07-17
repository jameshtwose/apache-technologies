# %%
import os
from uuid import uuid4

from confluent_kafka import Producer
from confluent_kafka.serialization import (
    StringSerializer,
    SerializationContext,
    MessageField,
)
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer

from dotenv import load_dotenv, find_dotenv

_ = load_dotenv(find_dotenv())


# %%
class User(object):
    """
    User record

    Args:
        name (str): User's name

        favorite_number (int): User's favorite number

        favorite_color (str): User's favorite color

        address(str): User's address; confidential
    """

    def __init__(self, name, address, favorite_number, favorite_color):
        self.name = name
        self.favorite_number = favorite_number
        self.favorite_color = favorite_color
        # address should not be serialized, see user_to_dict()
        self._address = address


# %%
def user_to_dict(user, ctx):
    """
    Returns a dict representation of a User instance for serialization.

    Args:
        user (User): User instance.

        ctx (SerializationContext): Metadata pertaining to the serialization
            operation.

    Returns:
        dict: Dict populated with user attributes to be serialized.
    """

    # User._address must not be serialized; omit from dict
    return dict(
        name=user.name,
        favorite_number=user.favorite_number,
        favorite_color=user.favorite_color,
    )


# %%
def delivery_report(err, msg):
    """
    Reports the failure or success of a message delivery.

    Args:
        err (KafkaError): The error that occurred on None on success.

        msg (Message): The message that was produced or failed.

    Note:
        In the delivery report callback the Message.key() and Message.value()
        will be the binary format as encoded by any configured Serializers and
        not the same object that was passed to produce().
        If you wish to pass the original object(s) for key and value to delivery
        report callback we recommend a bound callback or lambda where you pass
        the objects along.
    """

    if err is not None:
        print("Delivery failed for User record {}: {}".format(msg.key(), err))
        return
    print(
        "User record {} successfully produced to {} [{}] at offset {}".format(
            msg.key(), msg.topic(), msg.partition(), msg.offset()
        )
    )


# %%
sr_conf = {
    "url": os.environ["SCHEMA_REGISTRY_URL"],
    "basic.auth.user.info": f"{os.environ['SCHEMA_REGISTRY_API_KEY']}:{os.environ['SCHEMA_REGISTRY_API_SECRET']}",
}
schema = "user_specific.avsc"
path = os.path.realpath(os.path.dirname(__file__))
with open(f"{path}/avro/{schema}") as f:
    schema_str = f.read()
schema_registry_client = SchemaRegistryClient(sr_conf)

avro_serializer = AvroSerializer(schema_registry_client, schema_str, user_to_dict)

string_serializer = StringSerializer("utf_8")

producer_conf = {"bootstrap.servers": os.environ["BOOTSTRAP_SERVERS"],
                 "security.protocol": "SASL_SSL",
                 "sasl.mechanism": "PLAIN",
                 "sasl.username": os.environ["SASL_USERNAME"],
                 "sasl.password": os.environ["SASL_PASSWORD"],
                 "client.id": "avro_producer"}

producer = Producer(producer_conf)

# %%
schema_registry_client.get_subjects()
# %%
topic = "user_info"
# %%
print("Producing user records to topic {}. ^C to exit.".format(topic))
while True:
    # Serve on_delivery callbacks from previous calls to produce()
    producer.poll(0.0)
    try:
        user_name = input("Enter name: ")
        user_address = input("Enter address: ")
        user_favorite_number = int(input("Enter favorite number: "))
        user_favorite_color = input("Enter favorite color: ")
        user = User(
            name=user_name,
            address=user_address,
            favorite_color=user_favorite_color,
            favorite_number=user_favorite_number,
        )
        producer.produce(
            topic=topic,
            key=string_serializer(str(uuid4())),
            value=avro_serializer(
                user, SerializationContext(topic, MessageField.VALUE)
            ),
            on_delivery=delivery_report,
        )
    except KeyboardInterrupt:
        break
    except ValueError:
        print("Invalid input, discarding record...")
        continue

print("\nFlushing records...")
producer.flush()
# %%
