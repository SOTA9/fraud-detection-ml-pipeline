from pyspark.sql import DataFrame
from pyspark.sql.functions import window, col, avg, stddev, max, min, sum, when
import redis
import os
import json

# Redis configuration
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
TTL = 86400  # 24 hours

def compute_amount_ratio(avg_24h: float, current_amount: float) -> float:
    """
    Compute the ratio of the current transaction amount to the 24h average.
    """
    if avg_24h and avg_24h > 0:
        return round(current_amount / avg_24h, 4)
    return 1.0

def write_to_redis(batch_df: DataFrame, batch_id):
    """
    Write aggregated transaction amounts per user to Redis.
    """
    r = redis.Redis(host=REDIS_HOST, port=6379, decode_responses=True)
    pipe = r.pipeline()

    for row in batch_df.collect():
        key = f"feature:avg_amount_24h:{row.user_id}"
        value = json.dumps({
            "avg_amount": row.avg_amount,
            "stddev": row.stddev_amount,
            "max_amount": row.max_amount,
            "min_amount": row.min_amount,
            "sum_amount": row.sum_amount,
        })
        pipe.setex(key, TTL, value)

    pipe.execute()

def compute(df: DataFrame):
    """
    Compute 24h rolling statistics (avg, stddev, max, min, sum) for approved transactions
    and write to Redis.
    """
    approved = df.filter(col("status") == "APPROVED")

    agg = (
        approved.groupBy(
            col("user_id"),
            window(col("event_time"), "24 hours", "1 hour")
        )
        .agg(
            avg("amount").alias("avg_amount"),
            stddev("amount").alias("stddev_amount"),
            max("amount").alias("max_amount"),
            min("amount").alias("min_amount"),
            sum("amount").alias("sum_amount"),
        )
    )

    return (
        agg.writeStream
        .foreachBatch(write_to_redis)
        .outputMode("update")
        .option("checkpointLocation", "/tmp/checkpoints/f02")
        .start()
    )