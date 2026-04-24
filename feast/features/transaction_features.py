from datetime import timedelta
from feast import Entity, FeatureView, Field, PushSource, FileSource
from feast.types import Float64, Int64, Bool

# ------------------------------
# Entities
# ------------------------------

user_entity = Entity(
    name="user_id",
    description="Unique user identifier",
)

transaction_entity = Entity(
    name="transaction_id",
    description="Unique transaction identifier",
)

# ------------------------------
# Push Sources
# ------------------------------

user_push_source = PushSource(
    name="user_features_push",
    batch_source=FileSource(
        path="s3://my-feast-data/user_features/",
        timestamp_field="event_timestamp",
    ),
)

transaction_push_source = PushSource(
    name="transaction_features_push",
    batch_source=FileSource(
        path="s3://my-feast-data/transaction_features/",
        timestamp_field="event_timestamp",
    ),
)

# ------------------------------
# Feature Views
# ------------------------------

user_fraud_features = FeatureView(
    name="user_fraud_features",
    entities=[user_entity],
    ttl=timedelta(hours=24),
    schema=[
        Field(name="tx_last_5min_total", dtype=Int64),
        Field(name="tx_last_5min_approved", dtype=Int64),
        Field(name="tx_last_5min_failed", dtype=Int64),
        Field(name="avg_amount_24h", dtype=Float64),
        Field(name="stddev_amount_24h", dtype=Float64),
        Field(name="velocity_score", dtype=Float64),
        Field(name="distinct_devices_24h", dtype=Int64),
        Field(name="device_change_flag", dtype=Bool),
        Field(name="failed_attempts_30min", dtype=Int64),
        Field(name="hard_block", dtype=Bool),
    ],
    online=True,
    source=user_push_source,
)

transaction_fraud_features = FeatureView(
    name="transaction_fraud_features",
    entities=[transaction_entity],
    ttl=timedelta(minutes=10),
    schema=[
        Field(name="country_mismatch_flag", dtype=Bool),
        Field(name="country_risk_score", dtype=Float64),
        Field(name="is_unusual_country", dtype=Bool),
    ],
    online=True,
    source=transaction_push_source,
)