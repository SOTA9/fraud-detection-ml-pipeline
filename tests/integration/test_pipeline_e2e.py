import pytest
import time
import json
import redis
from testcontainers.kafka import KafkaContainer
from testcontainers.redis import RedisContainer
from confluent_kafka import Producer

# ------------------------------
# Configuration
# ------------------------------
NUM_TRANSACTIONS = 50

# ------------------------------
# Helper to generate a fake transaction
# ------------------------------
def make_transaction(i: int) -> dict:
    return {
        "transaction_id": f"tx-e2e-{i:04d}",
        "user_id": f"user-{i % 5}",
        "amount": round(10.0 + i * 1.5, 2),
        "currency": "EUR",
        "merchant_id": "merch-test",
        "merchant_country": "FR",
        "user_country": "FR" if i % 7 != 0 else "NG",
        "device_id": f"device-{i % 3}",
        "status": "APPROVED" if i % 4 != 0 else "FAILED",
        "failure_reason": None if i % 4 != 0 else "CARD_BLOCKED",
        "ip_address": "10.0.0.1",
        "event_time": int(time.time() * 1000),
        "metadata": {},
    }

# ------------------------------
# Testcontainers Fixtures
# ------------------------------
@pytest.fixture(scope="module")
def kafka_container():
    with KafkaContainer() as k:
        yield k

@pytest.fixture(scope="module")
def redis_container():
    with RedisContainer() as r:
        yield r

# ------------------------------
# End-to-End Test
# ------------------------------
def test_e2e_pipeline(kafka_container, redis_container):
    # Get connection info
    bootstrap = kafka_container.get_bootstrap_server()
    r_host = redis_container.get_container_host_ip()
    r_port = int(redis_container.get_exposed_port(6379))

    # Produce fake transactions to Kafka
    producer = Producer({"bootstrap.servers": bootstrap})
    for i in range(NUM_TRANSACTIONS):
        tx = make_transaction(i)
        producer.produce("raw-transactions", value=json.dumps(tx).encode())
    producer.flush()

    # Allow time for streaming micro-batches to process
    time.sleep(5)

    # Connect to Redis
    r = redis.Redis(host=r_host, port=r_port, decode_responses=True)

    # ------------------------------
    # Verify velocity features
    # ------------------------------
    keys = r.keys("feature:velocity:user-*")
    assert len(keys) > 0, "No velocity features written to Redis"
    for key in keys:
        data = json.loads(r.get(key))
        assert "velocity_score" in data
        assert 0.0 <= data["velocity_score"] <= 1.0
        assert "cnt_1m" in data

    # ------------------------------
    # Verify failed_attempts features
    # ------------------------------
    fa_keys = r.keys("feature:failed_attempts:user-*")
    assert len(fa_keys) > 0, "No failed_attempts features written"

    print(f"E2E passed: {len(keys)} velocity keys, {len(fa_keys)} failed_attempts keys")