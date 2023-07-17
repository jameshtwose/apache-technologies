# %%
import os
from confluent_kafka.schema_registry import SchemaRegistryClient
from dotenv import load_dotenv, find_dotenv

# %%
_ = load_dotenv(find_dotenv())

# %%
sr_conf = {
    "url": os.environ["SCHEMA_REGISTRY_URL"],
    "basic.auth.user.info": f"{os.environ['SCHEMA_REGISTRY_API_KEY']}:{os.environ['SCHEMA_REGISTRY_API_SECRET']}",
}
schema_registry_client = SchemaRegistryClient(sr_conf)
# %%
schema_registry_client.get_subjects()

# %%
schema_registry_client.get_latest_version("topic_0-value")
# %%
schema_registry_client.get_schema("topic_0-value")
# %%
schema_registry_client.get_schema(0)
# %%
