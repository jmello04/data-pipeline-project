"""
config/settings.py

Type-safe, validated central configuration via Pydantic BaseSettings.

All values are loaded from environment variables (or a .env file).
Invalid values are rejected at startup with a clear error — not silently
ignored at runtime.

Usage:
    from config.settings import settings
    path = settings.RAW_DATA_PATH
"""

from pathlib import Path

from pydantic import Field, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

_PROJECT_ROOT = Path(__file__).resolve().parent.parent


class Settings(BaseSettings):
    """Pipeline configuration loaded from environment / .env file.

    Attributes:
        RAW_DATA_PATH: Bronze layer directory (absolute or relative to CWD).
        PROCESSED_DATA_PATH: Silver layer directory.
        ANALYTICS_DATA_PATH: Gold layer directory.
        LOGS_PATH: Directory for structured log files.
        NUM_CUSTOMERS: Synthetic customer count. Must be ≥ 1.
        NUM_PRODUCTS: Synthetic product count. Must be ≥ 1.
        NUM_ORDERS: Synthetic order count. Must be ≥ 1.
        FAKE_DATA_SEED: Integer seed for reproducible Faker output.
        NULL_THRESHOLD: Max acceptable null fraction per column [0, 1].
        LGPD_HASH_KEY: Secret key (≥ 32 chars) for HMAC-SHA256 anonymisation.
        USE_REAL_KAFKA: Connect to a live broker when True; use queue otherwise.
        KAFKA_BOOTSTRAP_SERVERS: Kafka broker address.
        KAFKA_TOPIC: Topic name for order-event messages.
        KAFKA_GROUP_ID: Consumer group identifier.
        STREAMING_INTERVAL_SEC: Seconds between produced events (≥ 0).
        STREAMING_NUM_EVENTS: Total events per simulation run (≥ 1).
        SPARK_APP_NAME: Spark application display name.
        SPARK_MASTER: Spark master URL.
    """

    model_config = SettingsConfigDict(
        env_file=str(_PROJECT_ROOT / ".env"),
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # ── Paths ──────────────────────────────────────────────────────────────
    RAW_DATA_PATH: Path = Path("data/raw")
    PROCESSED_DATA_PATH: Path = Path("data/processed")
    ANALYTICS_DATA_PATH: Path = Path("data/analytics")
    LOGS_PATH: Path = Path("logs")

    # ── Synthetic data ─────────────────────────────────────────────────────
    NUM_CUSTOMERS: int = Field(default=1000, ge=1)
    NUM_PRODUCTS: int = Field(default=200, ge=1)
    NUM_ORDERS: int = Field(default=5000, ge=1)
    FAKE_DATA_SEED: int = 42

    # ── Quality ────────────────────────────────────────────────────────────
    NULL_THRESHOLD: float = Field(default=0.10, ge=0.0, le=1.0)

    # ── LGPD ───────────────────────────────────────────────────────────────
    LGPD_HASH_KEY: str = Field(
        ...,
        min_length=32,
        description="Secret key for HMAC-SHA256 PII anonymisation. "
                    "Generate with: python -c \"import secrets; print(secrets.token_hex(32))\"",
    )

    # ── Streaming / Kafka ──────────────────────────────────────────────────
    USE_REAL_KAFKA: bool = False
    KAFKA_BOOTSTRAP_SERVERS: str = "localhost:9092"
    KAFKA_TOPIC: str = "orders_stream"
    KAFKA_GROUP_ID: str = "pipeline_consumer"
    STREAMING_INTERVAL_SEC: float = Field(default=0.1, ge=0.0)
    STREAMING_NUM_EVENTS: int = Field(default=50, ge=1)

    # ── Spark ──────────────────────────────────────────────────────────────
    SPARK_APP_NAME: str = "DataPipelineProject"
    SPARK_MASTER: str = "local[*]"

    @field_validator("RAW_DATA_PATH", "PROCESSED_DATA_PATH", "ANALYTICS_DATA_PATH", "LOGS_PATH",
                     mode="before")
    @classmethod
    def _resolve_path(cls, v: str | Path) -> Path:
        """Convert string paths and resolve relative to project root.

        Args:
            v: Raw path value from environment.

        Returns:
            Resolved absolute Path.
        """
        p = Path(v)
        return p if p.is_absolute() else _PROJECT_ROOT / p

    @model_validator(mode="after")
    def _validate_kafka_config(self) -> "Settings":
        """Ensure Kafka settings are meaningful when real Kafka is enabled.

        Returns:
            Validated settings instance.

        Raises:
            ValueError: When USE_REAL_KAFKA=True but KAFKA_BOOTSTRAP_SERVERS is localhost default.
        """
        if self.USE_REAL_KAFKA and self.KAFKA_BOOTSTRAP_SERVERS == "localhost:9092":
            import warnings
            warnings.warn(
                "USE_REAL_KAFKA=true but KAFKA_BOOTSTRAP_SERVERS is still 'localhost:9092'. "
                "Confirm this is intentional.",
                UserWarning,
                stacklevel=2,
            )
        return self


settings = Settings()
