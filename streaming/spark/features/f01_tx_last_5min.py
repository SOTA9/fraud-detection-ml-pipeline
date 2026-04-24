from pyspark.sql import DataFrame
from pyspark.sql.functions import window, count, col, when
import redis
import os
import json

# Redis configuration
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
TTL = 600  # 5 min window + buffer


def write_to_redis(batch_df: DataFrame, batch_id):
    """
    Write aggregated batch data to Redis with TTL.
    """
    r = redis.Redis(host=REDIS_HOST, port=6379, decode_responses=True)
    pipe = r.pipeline()

    for row in batch_df.collect():
        key = f"feature:tx_5min:{row.user_id}"
        value = json.dumps({
            "total": row.total_tx,
            "approved": row.approved_tx,
            "failed": row.failed_tx,
            "window_start": str(row.window.start),
            "window_end": str(row.window.end),
        })
        pipe.setex(key, TTL, value)

    pipe.execute()


def compute(df: DataFrame):
    """
    Compute aggregated transaction metrics per user over 5-minute windows and write to Redis.
    """
    agg = (
        df.groupBy(
            col("user_id"),
            window(col("event_time"), "5 minutes", "1 minute")
        )
        .agg(
            count("*").alias("total_tx"),
            count(when(col("status") == "APPROVED", 1)).alias("approved_tx"),
            count(when(col("status") == "FAILED", 1)).alias("failed_tx"),
        )
    )

    return (
        agg.writeStream
        .foreachBatch(write_to_redis)
        .outputMode("update")
        .option("checkpointLocation", "/tmp/checkpoints/f01")
        .start()
    )