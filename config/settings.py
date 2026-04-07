"""
config/settings.py

Central configuration loader.  All pipeline modules must import settings from
here — never hardcode paths, credentials, or tuneable parameters elsewhere.

Usage:
    from config.settings import settings
    path = settings.RAW_DATA_PATH
"""

import os
from pathlib import Path
from dotenv import load_dotenv

# Load .env from project root (two levels up from this file)
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(_PROJECT_ROOT / ".env")


class Settings:
    """Typed, centralised access to all environment-driven configuration.

    Attributes:
        RAW_DATA_PATH: Directory for raw (Bronze) data files.
        PROCESSED_DATA_PATH: Directory for Silver-layer data files.
        ANALYTICS_DATA_PATH: Directory for Gold-layer (dimensional model) files.
        LOGS_PATH: Directory for log files.
        NUM_CUSTOMERS: Number of synthetic customer records to generate.
        NUM_PRODUCTS: Number of synthetic product records to generate.
        NUM_ORDERS: Number of synthetic order records to generate.
        FAKE_DATA_SEED: Random seed for reproducible Faker data.
        NULL_THRESHOLD: Maximum acceptable fraction of nulls per column (0–1).
        USE_REAL_KAFKA: Whether to connect to a live Kafka broker.
        KAFKA_BOOTSTRAP_SERVERS: Kafka broker address.
        KAFKA_TOPIC: Topic name for order events.
        KAFKA_GROUP_ID: Consumer group identifier.
        STREAMING_INTERVAL_SEC: Seconds between produced events (simulation).
        STREAMING_NUM_EVENTS: Total events to produce in one simulation run.
        SPARK_APP_NAME: Spark application display name.
        SPARK_MASTER: Spark master URL.
    """

    # ── Paths ──────────────────────────────────────────────────────────────
    RAW_DATA_PATH: Path = Path(os.getenv("RAW_DATA_PATH", "data/raw"))
    PROCESSED_DATA_PATH: Path = Path(os.getenv("PROCESSED_DATA_PATH", "data/processed"))
    ANALYTICS_DATA_PATH: Path = Path(os.getenv("ANALYTICS_DATA_PATH", "data/analytics"))
    LOGS_PATH: Path = Path(os.getenv("LOGS_PATH", "logs"))

    # ── Synthetic data ─────────────────────────────────────────────────────
    NUM_CUSTOMERS: int = int(os.getenv("NUM_CUSTOMERS", "1000"))
    NUM_PRODUCTS: int = int(os.getenv("NUM_PRODUCTS", "200"))
    NUM_ORDERS: int = int(os.getenv("NUM_ORDERS", "5000"))
    FAKE_DATA_SEED: int = int(os.getenv("FAKE_DATA_SEED", "42"))

    # ── Quality ────────────────────────────────────────────────────────────
    NULL_THRESHOLD: float = float(os.getenv("NULL_THRESHOLD", "0.10"))

    # ── Streaming / Kafka ──────────────────────────────────────────────────
    USE_REAL_KAFKA: bool = os.getenv("USE_REAL_KAFKA", "false").lower() == "true"
    KAFKA_BOOTSTRAP_SERVERS: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    KAFKA_TOPIC: str = os.getenv("KAFKA_TOPIC", "orders_stream")
    KAFKA_GROUP_ID: str = os.getenv("KAFKA_GROUP_ID", "pipeline_consumer")
    STREAMING_INTERVAL_SEC: float = float(os.getenv("STREAMING_INTERVAL_SEC", "0.5"))
    STREAMING_NUM_EVENTS: int = int(os.getenv("STREAMING_NUM_EVENTS", "50"))

    # ── Spark ──────────────────────────────────────────────────────────────
    SPARK_APP_NAME: str = os.getenv("SPARK_APP_NAME", "DataPipelineProject")
    SPARK_MASTER: str = os.getenv("SPARK_MASTER", "local[*]")


settings = Settings()
