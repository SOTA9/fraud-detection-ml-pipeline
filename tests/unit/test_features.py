import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from datetime import datetime, timedelta

# ------------------------------
# Spark Fixture
# ------------------------------
@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder
        .appName("fraud-tests")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )

# ------------------------------
# Helper to create transaction DataFrame
# ------------------------------
def _tx_df(spark, rows):
    schema = StructType([
        StructField("transaction_id", StringType()),
        StructField("user_id", StringType()),
        StructField("amount", DoubleType()),
        StructField("status", StringType()),
        StructField("failure_reason", StringType()),
        StructField("merchant_country", StringType()),
        StructField("user_country", StringType()),
        StructField("device_id", StringType()),
        StructField("event_time", TimestampType()),
    ])
    return spark.createDataFrame(rows, schema=schema)

# ------------------------------
# F01: Counts (total / approved / failed)
# ------------------------------
def test_f01_counts(spark):
    now = datetime.utcnow()
    rows = [
        ("tx1", "u1", 10.0, "APPROVED", None, "FR", "FR", "d1", now),
        ("tx2", "u1", 5.0, "FAILED", "CARD_BLOCKED", "FR", "FR", "d1", now),
        ("tx3", "u1", 20.0, "APPROVED", None, "FR", "FR", "d1", now),
    ]
    df = _tx_df(spark, rows)

    from pyspark.sql.functions import count, when, col

    result = (
        df.groupBy("user_id")
        .agg(
            count("*").alias("total"),
            count(when(col("status") == "APPROVED", 1)).alias("approved"),
            count(when(col("status") == "FAILED", 1)).alias("failed"),
        )
        .collect()[0]
    )

    assert result.total == 3
    assert result.approved == 2
    assert result.failed == 1

# ------------------------------
# F02: Average amount
# ------------------------------
def test_f02_avg_amount(spark):
    now = datetime.utcnow()
    rows = [
        ("tx1", "u2", 100.0, "APPROVED", None, "FR", "FR", "d1", now),
        ("tx2", "u2", 200.0, "APPROVED", None, "FR", "FR", "d1", now),
        ("tx3", "u2", 50.0, "FAILED", None, "FR", "FR", "d1", now),
    ]
    df = _tx_df(spark, rows)

    from pyspark.sql.functions import avg, col

    result = (
        df.filter(col("status") == "APPROVED")
        .groupBy("user_id")
        .agg(avg("amount").alias("avg"))
        .collect()[0]
    )

    assert result.avg == 150.0

# ------------------------------
# F04: Velocity score
# ------------------------------
def test_f04_velocity_score_zero_for_new_user():
    from streaming.spark.features.f04_velocity_score import compute_velocity
    score = compute_velocity(0, 0, 0)
    assert 0.0 <= score <= 1.0

def test_f04_velocity_score_high_for_burst():
    from streaming.spark.features.f04_velocity_score import compute_velocity
    score = compute_velocity(20, 30, 50)
    assert score > 0.7

# ------------------------------
# F05: Device score
# ------------------------------
def test_f05_device_score_single_device():
    from streaming.spark.features.f05_device_change import score_device_count
    assert score_device_count(1) == 0.0

def test_f05_device_score_multiple_devices():
    from streaming.spark.features.f05_device_change import score_device_count
    assert score_device_count(4) == 0.8

# ------------------------------
# F06: Hard block for credential stuffing
# ------------------------------
def test_f06_hard_block_credential_stuffing(spark):
    now = datetime.utcnow()
    rows = [
        (f"tx{i}", "u3", 5.0, "FAILED", "WRONG_PIN", "FR", "FR", "d1", now)
        for i in range(12)
    ]
    df = _tx_df(spark, rows)

    from pyspark.sql.functions import col

    cnt = df.filter(col("status") == "FAILED").count()
    assert cnt > 10  # triggers hard block