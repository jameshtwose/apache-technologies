# %%
import os
from dotenv import load_dotenv, find_dotenv
from confluent_kafka.admin import AdminClient

# %%
_ = load_dotenv(find_dotenv())

# %%
conf = {
    "bootstrap.servers": os.environ["BOOTSTRAP_SERVERS"],
    "security.protocol": "SASL_SSL",
    "sasl.mechanism": "PLAIN",
    "sasl.username": os.environ["SASL_USERNAME"],
    "sasl.password": os.environ["SASL_PASSWORD"],
}
kadmin = AdminClient(conf)

kadmin.list_topics().topics

# %%
