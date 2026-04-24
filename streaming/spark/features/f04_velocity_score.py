import math
from pyspark.sql import DataFrame
from pyspark.sql.functions import window, count, col
import redis
import os
import json

# Redis configuration
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
TTL = 300  # 5 minutes

def sigmoid(x: float) -> float:
    """
    Compute the sigmoid of x.
    """
    return 1 / (1 + math.exp(-x))

def compute_velocity(count_1m: int, count_5m: int, count_1h: int) -> float:
    """
    Compute a weighted velocity score based on counts over different time windows.
    """
    v1 = sigmoid(count_1m - 3)  # burst detection
    v5 = sigmoid(count_5m - 8)  # short-term pattern
    v1h = sigmoid(count_1h - 20)  # account takeover
    return round(0.5 * v1 + 0.3 * v5 + 0.2 * v1h, 4)

def write_to_redis(batch_df: DataFrame, batch_id):
    """
    Write velocity scores to Redis per user.
    """
    r = redis.Redis(host=REDIS_HOST, port=6379, decode_responses=True)
    pipe = r.pipeline()

    for row in batch_df.collect():
        key = f"feature:velocity:{row.user_id}"
        score = compute_velocity(row.cnt_1m, row.cnt_5m, row.cnt_1h)
        pipe.setex(key, TTL, json.dumps({
            "velocity_score": score,
            "cnt_1m": row.cnt_1m,
            "cnt_5m": row.cnt_5m,
            "cnt_1h": row.cnt_1h,
        }))

    pipe.execute()

def compute(df: DataFrame):
    """
    Compute transaction velocity features over multiple time windows
    and write them to Redis.
    """
    # Count transactions per user in different windows
    w1m = df.groupBy("user_id", window("event_time", "1 minute", "1 minute")) \
            .agg(count("*").alias("cnt_1m"))

    w5m = df.groupBy("user_id", window("event_time", "5 minutes", "1 minute")) \
            .agg(count("*").alias("cnt_5m"))

    w1h = df.groupBy("user_id", window("event_time", "1 hour", "5 minutes")) \
            .agg(count("*").alias("cnt_1h"))

    # Join all windows per user
    joined = (
        w1m.alias("a")
        .join(w5m.alias("b"), on="user_id", how="left")
        .join(w1h.alias("c"), on="user_id", how="left")
        .select("user_id", "cnt_1m", "cnt_5m", "cnt_1h")
        .fillna(0)
    )

    return (
        joined.writeStream
        .foreachBatch(write_to_redis)
        .outputMode("update")
        .option("checkpointLocation", "/tmp/checkpoints/f04")
        .start()
    )