import xgboost as xgb
import numpy as np
import pickle
import os
from loguru import logger

# ------------------------------
# Configuration
# ------------------------------
MODEL_PATH = os.getenv("MODEL_PATH", "ml/fraud_model.pkl")

FEATURE_ORDER = [
    "tx_last_5min_total",
    "tx_last_5min_failed",
    "avg_amount_24h",
    "velocity_score",
    "device_change_flag",
    "failed_attempts_30min",
    "country_risk_score",
]

# ------------------------------
# FraudModel Class
# ------------------------------

class FraudModel:
    """
    Wrapper around an XGBoost classifier for fraud prediction.
    Supports loading, saving, and predicting probabilities.
    """

    def __init__(self):
        self.model = self._load()

    def _load(self):
        """
        Load the model from disk. If not found, returns None (dummy model).
        """
        if os.path.exists(MODEL_PATH):
            with open(MODEL_PATH, "rb") as f:
                logger.info(f"Loaded model from {MODEL_PATH}")
                return pickle.load(f)
        logger.warning("No model file found — returning dummy model")
        return None

    def predict(self, features: dict) -> float:
        """
        Predict fraud probability given a dictionary of features.
        Returns a float between 0.0 and 1.0, rounded to 4 decimals.
        """
        if self.model is None:
            return 0.0

        # Construct feature vector in correct order
        vec = np.array([[features.get(f, 0.0) or 0.0 for f in FEATURE_ORDER]])

        # Predict probability of fraud (class 1)
        score = float(self.model.predict_proba(vec)[0][1])
        return round(score, 4)

    def save(self, model):
        """
        Save the model to disk.
        """
        with open(MODEL_PATH, "wb") as f:
            pickle.dump(model, f)
        logger.info(f"Model saved to {MODEL_PATH}")