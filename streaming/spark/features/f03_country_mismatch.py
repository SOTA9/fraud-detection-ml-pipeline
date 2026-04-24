from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, udf, broadcast
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, BooleanType
import redis
import os
import json
import psycopg2

# Configuration
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
PG_DSN = os.getenv("PG_DSN", "postgresql://fraud:fraud@localhost:5432/fraud")
TTL = 300  # 5 minutes
HIGH_RISK_PAIRS = {("NG", "US"), ("RO", "GB"), ("CN", "DE")}  # extend as needed


def country_risk_udf_fn(merchant_country, user_country):
    """
    Compute country mismatch and risk score for a transaction.
    """
    if not merchant_country or not user_country:
        return (False, 0.0, False)

    mismatch = merchant_country != user_country
    if (merchant_country, user_country) in HIGH_RISK_PAIRS:
        risk = 0.8
    elif mismatch:
        risk = 0.4
    else:
        risk = 0.0
    unusual = risk >= 0.8
    return (mismatch, float(risk), unusual)


country_risk_udf = udf(
    country_risk_udf_fn,
    StructType([
        StructField("mismatch", BooleanType()),
        StructField("risk_score", DoubleType()),
        StructField("is_unusual", BooleanType()),
    ])
)


def load_user_profiles(spark: SparkSession):
    """
    Load user profiles from PostgreSQL into a Spark DataFrame.
    """
    conn = psycopg2.connect(PG_DSN)
    cur = conn.cursor()
    cur.execute("SELECT user_id, country FROM user_profiles")
    rows = cur.fetchall()
    conn.close()

    schema = StructType([
        StructField("user_id", StringType()),
        StructField("home_country", StringType()),
    ])
    return spark.createDataFrame(rows, schema=schema)


def write_to_redis(batch_df: DataFrame, batch_id):
    """
    Write country risk features to Redis.
    """
    r = redis.Redis(host=REDIS_HOST, port=6379, decode_responses=True)
    pipe = r.pipeline()

    for row in batch_df.collect():
        key = f"feature:country_mismatch:{row.transaction_id}"
        value = json.dumps({
            "mismatch": row.mismatch,
            "risk_score": row.risk_score,
            "is_unusual": row.is_unusual,
        })
        pipe.setex(key, TTL, value)

    pipe.execute()


def compute(df: DataFrame, spark: SparkSession):
    """
    Compute country mismatch and risk score for streaming transactions
    and write results to Redis.
    """
    profiles = load_user_profiles(spark)
    enriched = df.join(broadcast(profiles), on="user_id", how="left")

    result = enriched.withColumn(
        "country_risk",
        country_risk_udf(col("merchant_country"), col("home_country"))
    ).select(
        "transaction_id",
        "user_id",
        col("country_risk.mismatch").alias("mismatch"),
        col("country_risk.risk_score").alias("risk_score"),
        col("country_risk.is_unusual").alias("is_unusual"),
    )

    return (
        result.writeStream
        .foreachBatch(write_to_redis)
        .outputMode("append")
        .option("checkpointLocation", "/tmp/checkpoints/f03")
        .start()
    )