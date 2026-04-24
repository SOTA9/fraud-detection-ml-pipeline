import pandas as pd
from feast import FeatureStore
from datetime import datetime, timedelta
import os

# ------------------------------
# Initialize Feast Feature Store
# ------------------------------
FEAST_REPO = os.getenv("FEAST_REPO_PATH", "./feast")
store = FeatureStore(repo_path=FEAST_REPO)


# ------------------------------
# Push Functions (from Spark foreachBatch)
# ------------------------------

def push_user_features(batch_df, batch_id):
    """
    Called from Spark foreachBatch — pushes user-level features
    to Redis (online store) and offline store.
    """
    rows = batch_df.toPandas()
    rows["event_timestamp"] = pd.Timestamp.utcnow()
    store.push("user_features_push", rows)


def push_transaction_features(batch_df, batch_id):
    """
    Called from Spark foreachBatch — pushes transaction-level features
    to Redis (online store) and offline store.
    """
    rows = batch_df.toPandas()
    rows["event_timestamp"] = pd.Timestamp.utcnow()
    store.push("transaction_features_push", rows)


# ------------------------------
# Real-time Feature Retrieval
# ------------------------------

def get_features_for_scoring(user_id: str, transaction_id: str) -> dict:
    """
    Retrieve online features in real-time (<5 ms) from Redis.
    Returns a dictionary of feature_name -> value.
    """
    feature_vector = store.get_online_features(
        features=[
            "user_fraud_features:tx_last_5min_total",
            "user_fraud_features:tx_last_5min_failed",
            "user_fraud_features:avg_amount_24h",
            "user_fraud_features:velocity_score",
            "user_fraud_features:device_change_flag",
            "user_fraud_features:failed_attempts_30min",
            "user_fraud_features:hard_block",
            "transaction_fraud_features:country_mismatch_flag",
            "transaction_fraud_features:country_risk_score",
        ],
        entity_rows=[{"user_id": user_id, "transaction_id": transaction_id}],
    ).to_dict()

    # Convert single-row result to simple dict
    return {k: v[0] for k, v in feature_vector.items()}


# ------------------------------
# Historical Dataset for Training
# ------------------------------

def get_training_dataset(start: datetime, end: datetime) -> pd.DataFrame:
    """
    Retrieve point-in-time correct historical features for model training.
    Prevents data leakage.
    """
    entity_df = pd.read_sql(
        "SELECT user_id, transaction_id, label, event_timestamp "
        "FROM fraud_features.labeled_transactions "
        f"WHERE event_timestamp BETWEEN '{start}' AND '{end}'",
        con=os.getenv("PG_DSN")
    )

    training_data = store.get_historical_features(
        entity_df=entity_df,
        features=[
            "user_fraud_features:avg_amount_24h",
            "user_fraud_features:velocity_score",
            "user_fraud_features:device_change_flag",
            "transaction_fraud_features:country_risk_score",
        ],
    ).to_df()

    return training_data