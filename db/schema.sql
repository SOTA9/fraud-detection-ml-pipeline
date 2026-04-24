-- ------------------------------
-- Create schema
-- ------------------------------
CREATE SCHEMA IF NOT EXISTS fraud_features;

-- ------------------------------
-- User-level aggregated features
-- ------------------------------
CREATE TABLE fraud_features.user_features (
    user_id VARCHAR(64) NOT NULL,
    event_timestamp TIMESTAMPTZ NOT NULL,
    tx_last_5min_total INT,
    tx_last_5min_approved INT,
    tx_last_5min_failed INT,
    avg_amount_24h DOUBLE PRECISION,
    stddev_amount_24h DOUBLE PRECISION,
    velocity_score DOUBLE PRECISION,
    distinct_devices_24h INT,
    device_change_flag BOOLEAN,
    failed_attempts_30min INT,
    hard_block BOOLEAN,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (user_id, event_timestamp)
);

-- ------------------------------
-- Per-transaction features
-- ------------------------------
CREATE TABLE fraud_features.transaction_features (
    transaction_id VARCHAR(64) NOT NULL,
    user_id VARCHAR(64) NOT NULL,
    event_timestamp TIMESTAMPTZ NOT NULL,
    country_mismatch_flag BOOLEAN,
    country_risk_score DOUBLE PRECISION,
    is_unusual_country BOOLEAN,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (transaction_id)
);

-- ------------------------------
-- Ground truth for model training
-- ------------------------------
CREATE TABLE fraud_features.labeled_transactions (
    transaction_id VARCHAR(64) NOT NULL PRIMARY KEY,
    user_id VARCHAR(64) NOT NULL,
    label SMALLINT NOT NULL CHECK (label IN (0, 1)),
    fraud_type VARCHAR(32),
    event_timestamp TIMESTAMPTZ NOT NULL,
    reviewed_at TIMESTAMPTZ,
    reviewer_id VARCHAR(64)
);

-- ------------------------------
-- Indexes for point-in-time joins
-- ------------------------------
CREATE INDEX idx_user_features_uid_ts
    ON fraud_features.user_features (user_id, event_timestamp DESC);

CREATE INDEX idx_tx_features_uid_ts
    ON fraud_features.transaction_features (user_id, event_timestamp DESC);

CREATE INDEX idx_labeled_uid_ts
    ON fraud_features.labeled_transactions (user_id, event_timestamp DESC);