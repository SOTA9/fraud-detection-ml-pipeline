import struct
import os
import json
from io import BytesIO

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, to_timestamp
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    LongType, MapType, IntegerType
)

from confluent_kafka.schema_registry import SchemaRegistryClient
from fastavro import schemaless_reader
from fastavro.schema import parse_schema

from loguru import logger

# ==============================
# CONFIG
# ==============================
KAFKA_BROKERS = os.getenv("BOOTSTRAP_SERVERS", "localhost:9092")
SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")
CHECKPOINT_DIR = os.getenv("CHECKPOINT_DIR", "/tmp/fraud_checkpoints")

TOPIC = "raw-transactions"
WATERMARK_DELAY = "10 minutes"

# ==============================
# TARGET SPARK SCHEMA
# ==============================
TX_SCHEMA = StructType([
    StructField("transaction_id", StringType()),
    StructField("user_id", StringType()),
    StructField("amount", DoubleType()),
    StructField("currency", StringType()),
    StructField("merchant_id", StringType()),
    StructField("merchant_country", StringType()),
    StructField("user_country", StringType()),
    StructField("device_id", StringType()),
    StructField("status", StringType()),
    StructField("failure_reason", StringType()),
    StructField("ip_address", StringType()),
    StructField("event_time", LongType()),
    StructField("metadata", MapType(StringType(), StringType())),
    StructField("_schema_id", IntegerType()),
])

# ==============================
# DECODER FACTORY (PRODUCTION SAFE)
# ==============================
def _make_decoder():
    """
    Executor-local decoder with schema cache.
    Handles schema evolution correctly using schema_id.
    """

    client = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
    schema_cache = {}

    def decode(wire_bytes: bytes):
        if wire_bytes is None or len(wire_bytes) < 5:
            return None

        # Validate magic byte
        if wire_bytes[0:1] != b"\x00":
            logger.warning(f"Invalid magic byte: {wire_bytes[0:1]!r}")
            return None

        # Extract schema ID
        schema_id = struct.unpack(">I", wire_bytes[1:5])[0]
        payload = wire_bytes[5:]

        try:
            # Fetch schema if not cached
            if schema_id not in schema_cache:
                schema_obj = client.get_schema(schema_id)
                parsed = parse_schema(json.loads(schema_obj.schema_str))
                schema_cache[schema_id] = parsed

                logger.info(f"Loaded schema_id={schema_id} into cache")

            # Decode Avro
            record = schemaless_reader(
                BytesIO(payload),
                schema_cache[schema_id]
            )

            # Add schema_id for auditing
            record["_schema_id"] = schema_id

            return record

        except Exception as e:
            logger.error(f"Decoding failed (schema_id={schema_id}): {e}")
            return None

    return decode

# ==============================
# SPARK SESSION
# ==============================
def create_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("FraudFeaturePipeline")
        .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_DIR)
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        .getOrCreate()
    )

# ==============================
# READ STREAM (UPGRADED)
# ==============================
def read_kafka_stream(spark: SparkSession):

    decode_udf = udf(_make_decoder(), TX_SCHEMA)

    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKERS)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "latest")
        .option("kafka.isolation.level", "read_committed")
        .option("failOnDataLoss", "false")
        .load()
    )

    df = (
        raw
        # Decode directly into struct (NO JSON)
        .withColumn("data", decode_udf(col("value")))
        .select("data.*")
        .where(col("transaction_id").isNotNull())  # filter bad records
        .withColumn("event_time", to_timestamp(col("event_time") / 1000))
        .withWatermark("event_time", WATERMARK_DELAY)
    )

    return df