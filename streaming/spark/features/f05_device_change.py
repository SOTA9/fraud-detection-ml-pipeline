from pyspark.sql import DataFrame
from pyspark.sql.functions import window, col, collect_set, size
import redis
import os
import json

# Redis configuration
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
TTL = 86400  # 24 hours

# Device score mapping
DEVICE_SCORE_MAP = {1: 0.0, 2: 0.3, 3: 0.6, 4: 0.8, 5: 0.9}

def score_device_count(n: int) -> float:
    """
    Map the number of distinct devices to a risk score.
    """
    if n > 5:
        return 0.9
    return DEVICE_SCORE_MAP.get(n, 0.0)

def write_to_redis(batch_df: DataFrame, batch_id):
    """
    Write device change features to Redis.
    """
    r = redis.Redis(host=REDIS_HOST, port=6379, decode_responses=True)
    pipe = r.pipeline()

    for row in batch_df.collect():
        n = row.distinct_devices
        key = f"feature:device_change:{row.user_id}"
        value = json.dumps({
            "device_change_flag": n > 1,
            "distinct_devices": n,
            "device_score": score_device_count(n),
            "known_devices": row.device_set,
        })
        pipe.setex(key, TTL, value)

    pipe.execute()

def compute(df: DataFrame):
    """
    Compute device change features over a 24-hour window and write to Redis.
    """
    agg = (
        df.filter(col("device_id").isNotNull())
        .groupBy(
            "user_id",
            window("event_time", "24 hours", "1 hour")
        )
        .agg(
            collect_set("device_id").alias("device_set"),
            size(collect_set("device_id")).alias("distinct_devices"),
        )
    )

    return (
        agg.writeStream
        .foreachBatch(write_to_redis)
        .outputMode("update")
        .option("checkpointLocation", "/tmp/checkpoints/f05")
        .start()
    )