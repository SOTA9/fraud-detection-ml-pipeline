import uuid
import os

from confluent_kafka import Producer, KafkaError
from confluent_kafka.serialization import SerializationContext, MessageField

from kafka.schema_registry.registry_client import (
    make_serializer,
    register_schema,
    get_schema_version,
)

from loguru import logger


class FraudTransactionProducer:

    def __init__(self, bootstrap_servers: str, schema_registry_url: str):

        # Wire the URL into the env so registry_client picks it up
        os.environ.setdefault("SCHEMA_REGISTRY_URL", schema_registry_url)

        # Register schema — logs schema_id + version at startup
        schema_id = register_schema()
        version = get_schema_version()

        logger.info(
            f"Producer ready  schema_id={schema_id}  version={version}"
        )

        self._serializer = make_serializer()
        self._ctx = SerializationContext(
            "raw-transactions",
            MessageField.VALUE,
        )

        self.topic = "raw-transactions"
        self.schema_id = schema_id

        self.producer = Producer(
            {
                "bootstrap.servers": bootstrap_servers,
                "enable.idempotence": True,
                "transactional.id": f"fraud-producer-{uuid.uuid4()}",
                "acks": "all",
                "retries": 10,
                "max.in.flight.requests.per.connection": 5,
            }
        )

        self.producer.init_transactions()

    def _serialize(self, record: dict) -> bytes:
        # AvroSerializer handles magic byte + schema_id + Avro encoding
        return self._serializer(record, self._ctx)

    def send_transaction(self, transaction: dict) -> None:

        key = transaction["user_id"].encode()
        value = self._serialize(transaction)

        try:
            self.producer.begin_transaction()

            self.producer.produce(
                topic=self.topic,
                key=key,
                value=value,
                on_delivery=self._delivery_report,
            )

            self.producer.commit_transaction()

        except KafkaError as e:
            logger.error(f"Kafka error: {e}")
            self.producer.abort_transaction()
            raise

    def send_batch(self, transactions: list[dict]) -> None:

        try:
            self.producer.begin_transaction()

            for tx in transactions:
                self.producer.produce(
                    topic=self.topic,
                    key=tx["user_id"].encode(),
                    value=self._serialize(tx),
                    on_delivery=self._delivery_report,
                )

            self.producer.flush()
            self.producer.commit_transaction()

            logger.info(
                f"Batch of {len(transactions)} committed "
                f"schema_id={self.schema_id}"
            )

        except KafkaError as e:
            logger.error(f"Batch failed: {e}")
            self.producer.abort_transaction()
            raise

    @staticmethod
    def _delivery_report(err, msg):

        if err:
            logger.error(f"Delivery failed: {err}")
        else:
            logger.debug(
                f"Delivered -> topic={msg.topic()} "
                f"partition={msg.partition()} "
                f"offset={msg.offset()}"
            )

    def close(self):
        self.producer.flush()