from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from confluent_kafka import Producer
from feast.push_and_serve import get_features_for_scoring
from ml.model import FraudModel
import json
import os
import time
from loguru import logger

# ------------------------------
# App & Model Initialization
# ------------------------------
app = FastAPI(title="Fraud Scoring API", version="1.0.0")
model = FraudModel()

# Kafka configuration
KAFKA_BROKERS = os.getenv("BOOTSTRAP_SERVERS", "localhost:9092")
FRAUD_TOPIC = "fraud-alerts"
CLEARED_TOPIC = "cleared-transactions"
FRAUD_THRESHOLD = 0.85

producer = Producer({"bootstrap.servers": KAFKA_BROKERS, "acks": "all"})


# ------------------------------
# Request & Response Models
# ------------------------------
class TransactionRequest(BaseModel):
    transaction_id: str
    user_id: str
    amount: float
    merchant_country: str
    user_country: str


class ScoreResponse(BaseModel):
    transaction_id: str
    score: float
    decision: str
    latency_ms: float


# ------------------------------
# Helper function to publish to Kafka
# ------------------------------
def _publish(topic: str, tx_id: str, score: float, decision: str):
    payload = json.dumps({
        "transaction_id": tx_id,
        "score": score,
        "decision": decision
    })
    producer.produce(topic, value=payload.encode())
    producer.poll(0)


# ------------------------------
# Scoring Endpoint
# ------------------------------
@app.post("/score", response_model=ScoreResponse)
async def score(tx: TransactionRequest):
    t0 = time.perf_counter()

    # Fetch features from Feast / Redis
    try:
        features = get_features_for_scoring(tx.user_id, tx.transaction_id)
    except Exception as e:
        logger.error(f"Feature fetch failed: {e}")
        raise HTTPException(status_code=503, detail="Feature store unavailable")

    # Hard block short-circuit
    if features.get("hard_block"):
        _publish(FRAUD_TOPIC, tx.transaction_id, 1.0, "BLOCKED")
        return ScoreResponse(
            transaction_id=tx.transaction_id,
            score=1.0,
            decision="BLOCKED",
            latency_ms=round((time.perf_counter() - t0) * 1000, 2)
        )

    # Predict fraud probability
    score_value = model.predict(features)
    decision = "FRAUD" if score_value >= FRAUD_THRESHOLD else "CLEARED"
    topic = FRAUD_TOPIC if score_value >= FRAUD_THRESHOLD else CLEARED_TOPIC

    # Publish to Kafka
    _publish(topic, tx.transaction_id, score_value, decision)

    return ScoreResponse(
        transaction_id=tx.transaction_id,
        score=score_value,
        decision=decision,
        latency_ms=round((time.perf_counter() - t0) * 1000, 2)
    )


# ------------------------------
# Health Check
# ------------------------------
@app.get("/health")
async def health():
    return {"status": "ok"}