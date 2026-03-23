import os
from functools import lru_cache
from confluent_kafka.schema_registry import SchemaRegistryClient, Schema
from confluent_kafka.schema_registry.avro import AvroSerializer
from loguru import logger

SCHEMA_REGISTRY_URL = os.getenv(
    "SCHEMA_REGISTRY_URL", "http://localhost:8081"
)

BASE_DIR    = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
SCHEMA_FILE = os.path.join(BASE_DIR, "schemas", "transaction.avsc")

TOPIC   = "raw-transactions"
SUBJECT = f"{TOPIC}-value"


@lru_cache(maxsize=1)
def get_registry_client() -> SchemaRegistryClient:
    client = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
    # FIX 2: loguru does not accept keyword args — embed in f-string
    logger.info(f"Schema Registry client initialized  url={SCHEMA_REGISTRY_URL}")
    return client


def register_schema() -> int:
    client = get_registry_client()

    with open(SCHEMA_FILE) as f:
        schema_str = f.read()

    schema    = Schema(schema_str, schema_type="AVRO")
    schema_id = client.register_schema(SUBJECT, schema)
    latest    = client.get_latest_version(SUBJECT)

    logger.info(
        f"Schema registered  subject={SUBJECT}  "
        f"schema_id={schema_id}  version={latest.version}"
    )
    return schema_id


def to_dict(obj, ctx):
    return obj


def make_serializer() -> AvroSerializer:
    with open(SCHEMA_FILE) as f:
        schema_str = f.read()

    return AvroSerializer(
        schema_registry_client=get_registry_client(),
        schema_str=schema_str,
        to_dict=to_dict,
        conf={"auto.register.schemas": True},
    )