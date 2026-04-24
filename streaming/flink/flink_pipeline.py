import os
import json
from pyflink.datastream import StreamExecutionEnvironment, TimeCharacteristic
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer
from pyflink.common.serialization import DeserializationSchema
from pyflink.common.typeinfo import Types
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.common.time import Duration
from pyflink.datastream.window import TumblingEventTimeWindows
from pyflink.common.time import Time

from confluent_kafka.schema_registry.avro import AvroDeserializer
from kafka.schema_registry.registry_client import get_registry_client
from loguru import logger

KAFKA_BROKERS = os.getenv("BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = "raw-transactions"


class AvroDeserializationSchemaWrapper(DeserializationSchema):
    """
    Wrap Confluent AvroDeserializer into PyFlink DeserializationSchema
    """

    def __init__(self, avro_deserializer: AvroDeserializer):
        self._avro_deserializer = avro_deserializer

    def deserialize(self, message: bytes):
        if message is None:
            return None
        # Convert Avro bytes to dict
        return self._avro_deserializer(message, None)

    def is_end_of_stream(self, next_element):
        return False

    def get_produced_type(self):
        # dict type in PyFlink
        return Types.PICKLED_BYTE_ARRAY()


def run():
    # 1️⃣ Create Flink environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_stream_time_characteristic(TimeCharacteristic.EventTime)
    env.set_parallelism(4)

    # 2️⃣ Kafka consumer config
    kafka_props = {
        "bootstrap.servers": KAFKA_BROKERS,
        "group.id": "flink-fraud-consumer",
        "auto.offset.reset": "latest",
    }

    # 3️⃣ Avro deserializer using your schema registry
    avro_deserializer = AvroDeserializer(
        schema_registry_client=get_registry_client(),
        schema_str=open(os.path.join(os.path.dirname(__file__), "..", "schemas", "transaction.avsc")).read(),
        to_dict=lambda obj, ctx: obj,
    )
    deserialization_schema = AvroDeserializationSchemaWrapper(avro_deserializer)

    # 4️⃣ Create Kafka source
    consumer = FlinkKafkaConsumer(
        topics=TOPIC,
        deserialization_schema=deserialization_schema,
        properties=kafka_props,
    )

    # 5️⃣ Watermarks + event-time
    watermark_strategy = (
        WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_seconds(10))
        .with_timestamp_assigner(lambda tx, _: tx.get("event_time", 0))
    )

    stream = (
        env.add_source(consumer)
        .assign_timestamps_and_watermarks(watermark_strategy)
    )

    # 6️⃣ Compute velocity feature (1-min tumbling window)
    velocity = (
        stream
        .key_by(lambda tx: tx["user_id"])
        .window(TumblingEventTimeWindows.of(Time.minutes(1)))
        .reduce(lambda a, b: {**a, "count": a.get("count", 1) + 1})
    )

    # 7️⃣ Output to console (for testing)
    velocity.print()

    # 8️⃣ Execute pipeline
    env.execute("FraudFeaturePipelineFlink")


if __name__ == "__main__":
    run()