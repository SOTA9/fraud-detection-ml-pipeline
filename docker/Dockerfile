# ------------------------------
# Base image: Spark with Python
# ------------------------------
FROM bitnami/spark:3.5.1

# Switch to root to install pip
USER root

# Install pip for Python 3
RUN install_packages python3-pip

# ------------------------------
# Set working directory
# ------------------------------
WORKDIR /app

# Copy and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the application code
COPY . .

# ------------------------------
# Environment variables
# ------------------------------
ENV PYTHONPATH=/app
ENV PYSPARK_PYTHON=python3

# ------------------------------
# Entry point: Spark-submit with Kafka package
# ------------------------------
ENTRYPOINT [
    "spark-submit",
    "--packages",
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1",
    "streaming/spark/fraud_features_session.py"
]