from pyspark.sql import DataFrame
from pyspark.sql.functions import window, col, count, when
import redis
import os
import json

# Redis configuration
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
TTL = 1800  # 30 minutes

# Thresholds
CREDENTIAL_STUFF_THR = 10
CARD_TESTING_AMOUNT = 5.0
CARD_TESTING_FAILS = 3

def write_to_redis(batch_df: DataFrame, batch_id):
    """
    Write failed attempts and fraud-related features to Redis.
    """
    r = redis.Redis(host=REDIS_HOST, port=6379, decode_responses=True)
    pipe = r.pipeline()

    for row in batch_df.collect():
        hard_block = (
            row.total_failed > CREDENTIAL_STUFF_THR or
            row.small_amount_fails > CARD_TESTING_FAILS
        )

        key = f"feature:failed_attempts:{row.user_id}"
        value = json.dumps({
            "failed_count": row.total_failed,
            "declined_count": row.total_declined,
            "insufficient_funds": row.insufficient_funds,
            "card_blocked": row.card_blocked,
            "small_amount_fails": row.small_amount_fails,
            "credential_stuffing": row.total_failed > CREDENTIAL_STUFF_THR,
            "card_testing": row.small_amount_fails > CARD_TESTING_FAILS,
            "hard_block": hard_block,
        })

        pipe.setex(key, TTL, value)

    pipe.execute()

def compute(df: DataFrame):
    """
    Compute failed attempts, card testing, and credential stuffing features over a 30-minute window
    and write to Redis.
    """
    agg = (
        df.groupBy("user_id", window("event_time", "30 minutes", "5 minutes"))
        .agg(
            count(when(col("status") == "FAILED", 1)).alias("total_failed"),
            count(when(col("status") == "DECLINED", 1)).alias("total_declined"),
            count(when(col("failure_reason") == "INSUFFICIENT_FUNDS", 1)).alias("insufficient_funds"),
            count(when(col("failure_reason") == "CARD_BLOCKED", 1)).alias("card_blocked"),
            count(when(
                (col("status") == "FAILED") & (col("amount") < CARD_TESTING_AMOUNT), 1
            )).alias("small_amount_fails"),
        )
    )

    return (
        agg.writeStream
        .foreachBatch(write_to_redis)
        .outputMode("update")
        .option("checkpointLocation", "/tmp/checkpoints/f06")
        .start()
    )