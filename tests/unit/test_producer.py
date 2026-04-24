import struct
import pytest
from unittest.mock import MagicMock, patch
from confluent_kafka import KafkaError, KafkaException

# Sample transaction for testing
SAMPLE_TX = {
    "transaction_id": "tx-test-001",
    "user_id": "user-42",
    "amount": 99.99,
    "currency": "EUR",
    "merchant_id": "merch-1",
    "merchant_country": "FR",
    "user_country": "FR",
    "device_id": "device-abc",
    "status": "APPROVED",
    "failure_reason": None,
    "ip_address": "192.168.1.1",
    "event_time": 1700000000000,
    "metadata": {},
}

FAKE_SCHEMA_ID = 42


def _wire(schema_id=FAKE_SCHEMA_ID):
    """Return fake Confluent wire-format payload."""
    return b"\x00" + struct.pack(">I", schema_id) + b"avro_payload"


# ------------------------------
# Test: Magic byte + schema_id in produced value
# ------------------------------
@patch("kafka.schema_registry.registry_client.register_schema", return_value=FAKE_SCHEMA_ID)
@patch("kafka.schema_registry.registry_client.get_schema_version", return_value=1)
@patch("kafka.schema_registry.registry_client.make_serializer")
@patch("confluent_kafka.Producer")
def test_wire_format_magic_byte(mock_prod, mock_ser_fn, mock_ver, mock_reg):
    mock_ser_fn.return_value = MagicMock(return_value=_wire())
    mock_prod.return_value = MagicMock()

    from kafka.producers.transaction_producer import FraudTransactionProducer
    p = FraudTransactionProducer("localhost:9092", "http://localhost:8081")
    p.send_transaction(SAMPLE_TX)

    # Capture value sent to producer
    _, kwargs = mock_prod.return_value.produce.call_args
    value = kwargs["value"]

    # Check magic byte
    assert value[0:1] == b"\x00", "Missing Confluent magic byte"

    # Check schema ID
    actual_sid = struct.unpack(">I", value[1:5])[0]
    assert actual_sid == FAKE_SCHEMA_ID, f"schema_id mismatch: {actual_sid}"


# ------------------------------
# Test: schema_id stored on producer
# ------------------------------
@patch("kafka.schema_registry.registry_client.register_schema", return_value=FAKE_SCHEMA_ID)
@patch("kafka.schema_registry.registry_client.get_schema_version", return_value=1)
@patch("kafka.schema_registry.registry_client.make_serializer")
@patch("confluent_kafka.Producer")
def test_schema_id_stored_on_producer(mock_prod, mock_ser_fn, mock_ver, mock_reg):
    mock_ser_fn.return_value = MagicMock(return_value=_wire())
    mock_prod.return_value = MagicMock()

    from kafka.producers.transaction_producer import FraudTransactionProducer
    p = FraudTransactionProducer("localhost:9092", "http://localhost:8081")

    assert p.schema_id == FAKE_SCHEMA_ID
    assert p.schema_version == 1


# ------------------------------
# Test: abort transaction on KafkaError
# ------------------------------
@patch("kafka.schema_registry.registry_client.register_schema", return_value=FAKE_SCHEMA_ID)
@patch("kafka.schema_registry.registry_client.get_schema_version", return_value=1)
@patch("kafka.schema_registry.registry_client.make_serializer")
@patch("confluent_kafka.Producer")
def test_abort_on_kafka_error(mock_prod, mock_ser_fn, mock_ver, mock_reg):
    mock_producer = MagicMock()
    mock_producer.produce.side_effect = KafkaException(KafkaError(1, "err"))
    mock_prod.return_value = mock_producer
    mock_ser_fn.return_value = MagicMock(return_value=_wire())

    from kafka.producers.transaction_producer import FraudTransactionProducer
    p = FraudTransactionProducer("localhost:9092", "http://localhost:8081")

    with pytest.raises(KafkaException):
        p.send_transaction(SAMPLE_TX)

    mock_producer.abort_transaction.assert_called_once()


# ------------------------------
# Test: send_batch commits all transactions
# ------------------------------
@patch("kafka.schema_registry.registry_client.register_schema", return_value=FAKE_SCHEMA_ID)
@patch("kafka.schema_registry.registry_client.get_schema_version", return_value=1)
@patch("kafka.schema_registry.registry_client.make_serializer")
@patch("confluent_kafka.Producer")
def test_send_batch_success(mock_prod, mock_ser_fn, mock_ver, mock_reg):
    mock_producer = MagicMock()
    mock_prod.return_value = mock_producer
    mock_ser_fn.return_value = MagicMock(return_value=_wire())

    from kafka.producers.transaction_producer import FraudTransactionProducer
    p = FraudTransactionProducer("localhost:9092", "http://localhost:8081")

    txs = [SAMPLE_TX, SAMPLE_TX.copy()]
    p.send_batch(txs)

    # produce called twice
    assert mock_producer.produce.call_count == 2
    mock_producer.commit_transaction.assert_called_once()
    mock_producer.abort_transaction.assert_not_called()


# ------------------------------
# Test: send_batch aborts on KafkaError
# ------------------------------
@patch("kafka.schema_registry.registry_client.register_schema", return_value=FAKE_SCHEMA_ID)
@patch("kafka.schema_registry.registry_client.get_schema_version", return_value=1)
@patch("kafka.schema_registry.registry_client.make_serializer")
@patch("confluent_kafka.Producer")
def test_send_batch_abort(mock_prod, mock_ser_fn, mock_ver, mock_reg):
    mock_producer = MagicMock()
    mock_producer.produce.side_effect = [None, KafkaException(KafkaError(1, "err"))]
    mock_prod.return_value = mock_producer
    mock_ser_fn.return_value = MagicMock(return_value=_wire())

    from kafka.producers.transaction_producer import FraudTransactionProducer
    p = FraudTransactionProducer("localhost:9092", "http://localhost:8081")

    with pytest.raises(KafkaException):
        p.send_batch([SAMPLE_TX, SAMPLE_TX.copy()])

    mock_producer.abort_transaction.assert_called_once()