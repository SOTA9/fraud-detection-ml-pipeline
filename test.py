import uuid
import os
import time
from typing import List, Optional

from confluent_kafka import Producer, KafkaError
from confluent_kafka.serialization import SerializationContext, MessageField

from kafka.schema_registry.registry_client import (
    make_serializer,
    register_schema,
    get_schema_version,
)
from loguru import logger


class FraudTransactionProducer:
    """
    Production-ready Kafka producer for fraud transactions with:
    - Exactly-once semantics (EOS)
    - Avro serialization with schema registry
    - Single or batch transactions
    - Automatic retries with exponential backoff
    - Delivery logging and monitoring

    Note: Confluent Kafka Producer with transactions is NOT fully thread-safe.
    Each thread should have its own producer instance.
    """

    def __init__(
        self,
        bootstrap_servers: str,
        schema_registry_url: str,
        topic: str = "raw-transactions",
        transactional_id: Optional[str] = None,
        retries: int = 5,
        request_timeout_ms: int = 30000,
        delivery_timeout_ms: int = 30000,
    ):
        # Schema registry environment
        os.environ.setdefault("SCHEMA_REGISTRY_URL", schema_registry_url)

        # Schema registration
        self.schema_id = register_schema()
        self.schema_version = get_schema_version()
        logger.info(f"Producer ready  schema_id={self.schema_id}  version={self.schema_version}")

        # Serializer setup
        self._serializer = make_serializer()
        self._ctx = SerializationContext(topic, MessageField.VALUE)

        # Kafka configuration
        self.topic = topic
        self.retries = retries
        self.producer = Producer(
            {
                "bootstrap.servers": bootstrap_servers,
                "enable.idempotence": True,
                "transactional.id": transactional_id or f"fraud-producer-{uuid.uuid4()}",
                "acks": "all",
                "retries": 10,
                "max.in.flight.requests.per.connection": 5,
                "request.timeout.ms": request_timeout_ms,
                "delivery.timeout.ms": delivery_timeout_ms,
            }
        )

        self.producer.init_transactions()

    def _serialize(self, record: dict) -> bytes:
        """Convert Python dict to Avro bytes with magic byte + schema ID."""
        return self._serializer(record, self._ctx)

    def _produce_with_retry(self, key: bytes, value: bytes):
        """Produce a message with retry + exponential backoff."""
        backoff = 1  # initial backoff in seconds
        for attempt in range(self.retries):
            try:
                self.producer.produce(
                    topic=self.topic,
                    key=key,
                    value=value,
                    on_delivery=self._delivery_report,
                )
                return
            except KafkaError as e:
                logger.warning(f"Produce attempt {attempt + 1} failed: {e}. Retrying in {backoff}s.")
                time.sleep(backoff)
                backoff *= 2
        raise KafkaError(f"Failed to produce message after {self.retries} retries.")

    def send_transaction(self, transaction: dict) -> None:
        """Send a single transaction atomically."""
        key = transaction["user_id"].encode()
        value = self._serialize(transaction)
        try:
            self.producer.begin_transaction()
            self._produce_with_retry(key, value)
            self.producer.commit_transaction()
        except KafkaError as e:
            logger.error(f"Kafka error during send_transaction: {e}")
            self.producer.abort_transaction()
            raise

    def send_batch(self, transactions: List[dict]) -> None:
        """Send multiple transactions in a single atomic transaction."""
        try:
            self.producer.begin_transaction()
            for tx in transactions:
                key = tx["user_id"].encode()
                value = self._serialize(tx)
                self._produce_with_retry(key, value)

            self.producer.flush()
            self.producer.commit_transaction()
            logger.info(f"Batch of {len(transactions)} committed schema_id={self.schema_id}")
        except KafkaError as e:
            logger.error(f"Batch failed: {e}")
            self.producer.abort_transaction()
            raise

    @staticmethod
    def _delivery_report(err, msg):
        """Delivery callback to log success or failure."""
        if err:
            logger.error(f"Delivery failed: {err}")
        else:
            logger.debug(
                f"Delivered -> topic={msg.topic()} "
                f"partition={msg.partition()} "
                f"offset={msg.offset()}"
            )

    def close(self):
        """Flush and close producer safely."""
        try:
            self.producer.flush()
        finally:
            self.producer.close()
            logger.info("Producer closed successfully.")