import redis
import json
import os
from typing import Any, List, Tuple, Optional, Union

# ------------------------------
# Redis configuration
# ------------------------------
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))

# TTLs per feature
TTL_MAP = {
    "tx_5min": 600,
    "avg_amount_24h": 86400,
    "country_mismatch": 300,
    "velocity": 300,
    "device_change": 86400,
    "failed_attempts": 1800,
}

# ------------------------------
# Redis Feature Store Wrapper
# ------------------------------

class FraudFeatureRedisStore:
    """
    Wrapper around Redis for storing and retrieving fraud-related features.
    Supports single and batch get/set operations with TTL management.
    """

    def __init__(self):
        self.client = redis.Redis(
            host=REDIS_HOST,
            port=REDIS_PORT,
            decode_responses=True,
            socket_connect_timeout=1,
            socket_timeout=1,
        )

    def _key(self, feature_name: str, entity_id: str) -> str:
        """Generate a Redis key for a feature and entity."""
        return f"feature:{feature_name}:{entity_id}"

    def set(self, feature_name: str, entity_id: str, value: dict) -> None:
        """Set a single feature in Redis with TTL."""
        ttl = TTL_MAP.get(feature_name, 600)
        self.client.setex(self._key(feature_name, entity_id), ttl, json.dumps(value))

    def get(self, feature_name: str, entity_id: str) -> Optional[dict]:
        """Retrieve a single feature from Redis."""
        raw = self.client.get(self._key(feature_name, entity_id))
        return json.loads(raw) if raw else None

    def mget(self, feature_name: str, entity_ids: List[str]) -> List[Optional[dict]]:
        """Retrieve multiple features from Redis."""
        keys = [self._key(feature_name, eid) for eid in entity_ids]
        pipe = self.client.pipeline()
        for k in keys:
            pipe.get(k)
        results = pipe.execute()
        return [json.loads(r) if r else None for r in results]

    def mset(self, feature_name: str, records: List[Tuple[str, dict]]) -> None:
        """Set multiple features in Redis with TTL."""
        ttl = TTL_MAP.get(feature_name, 600)
        pipe = self.client.pipeline()
        for entity_id, value in records:
            pipe.setex(self._key(feature_name, entity_id), ttl, json.dumps(value))
        pipe.execute()