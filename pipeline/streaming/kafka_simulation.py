"""
pipeline/streaming/kafka_simulation.py

Streaming layer: Kafka producer/consumer simulation for real-time order events.

When USE_REAL_KAFKA=false (default), a Python queue is used as a stand-in so
the module works without a live Kafka broker.  Set USE_REAL_KAFKA=true in .env
and provide KAFKA_BOOTSTRAP_SERVERS to connect to a real cluster.

Run:
    python pipeline/streaming/kafka_simulation.py
"""

import json
import logging
import queue
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import Any

from faker import Faker

from config.settings import settings

# ── Logging ────────────────────────────────────────────────────────────────────
settings.LOGS_PATH.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s – %(message)s",
    handlers=[
        logging.FileHandler(settings.LOGS_PATH / "streaming.log"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)

fake = Faker("pt_BR")
Faker.seed(settings.FAKE_DATA_SEED)

_STREAMING_OUTPUT = settings.PROCESSED_DATA_PATH / "streaming"
_STREAMING_OUTPUT.mkdir(parents=True, exist_ok=True)


# ─────────────────────────────────────────────────────────────────────────────
# Event schema
# ─────────────────────────────────────────────────────────────────────────────


def _generate_order_event() -> dict[str, Any]:
    """Generate a single synthetic order event payload.

    Returns:
        Dictionary representing one order event.
    """
    return {
        "order_id": fake.uuid4(),
        "customer_id": fake.random_int(min=1, max=settings.NUM_CUSTOMERS),
        "product_id": fake.random_int(min=1, max=settings.NUM_PRODUCTS),
        "quantity": fake.random_int(min=1, max=5),
        "unit_price": round(fake.pyfloat(min_value=10, max_value=1500, right_digits=2), 2),
        "channel": fake.random_element(["web", "mobile", "marketplace"]),
        "event_time": datetime.utcnow().isoformat(),
    }


# ─────────────────────────────────────────────────────────────────────────────
# In-process queue backend (USE_REAL_KAFKA=false)
# ─────────────────────────────────────────────────────────────────────────────

_local_queue: queue.Queue[str] = queue.Queue()


class LocalProducer:
    """Simulated Kafka producer backed by a Python queue.

    Args:
        topic: Topic name (informational only in simulation mode).
    """

    def __init__(self, topic: str) -> None:
        self.topic = topic

    def send(self, payload: dict[str, Any]) -> None:
        """Enqueue a serialised event payload.

        Args:
            payload: Event dictionary to enqueue.
        """
        message = json.dumps(payload)
        _local_queue.put(message)
        logger.debug("LocalProducer → [%s] %s", self.topic, message[:60])

    def close(self) -> None:
        """No-op for the local backend."""


class LocalConsumer:
    """Simulated Kafka consumer backed by a Python queue.

    Args:
        topic: Topic name (informational only in simulation mode).
        group_id: Consumer group (informational only).
    """

    def __init__(self, topic: str, group_id: str) -> None:
        self.topic = topic
        self.group_id = group_id
        self._running = False

    def start(self, output_path: Path, max_messages: int | None = None) -> None:
        """Consume messages from the local queue and persist them.

        Args:
            output_path: Directory where JSONL output is written.
            max_messages: Stop after this many messages (None = run until
                queue is empty and producer signals done).
        """
        self._running = True
        records: list[dict[str, Any]] = []
        consumed = 0
        output_path.mkdir(parents=True, exist_ok=True)

        logger.info("LocalConsumer started [topic=%s group=%s]", self.topic, self.group_id)

        while self._running:
            try:
                raw = _local_queue.get(timeout=2)
                record = json.loads(raw)
                records.append(record)
                consumed += 1
                logger.info("Consumed event #%d: order_id=%s", consumed, record.get("order_id"))
                _local_queue.task_done()
                if max_messages and consumed >= max_messages:
                    break
            except queue.Empty:
                break

        # Persist batch
        if records:
            ts = datetime.utcnow().strftime("%Y%m%dT%H%M%S")
            out_file = output_path / f"stream_batch_{ts}.jsonl"
            with out_file.open("w") as fh:
                for r in records:
                    fh.write(json.dumps(r) + "\n")
            logger.info("Consumer wrote %d records → %s", len(records), out_file)

        self._running = False

    def close(self) -> None:
        """Signal the consumer loop to stop."""
        self._running = False


# ─────────────────────────────────────────────────────────────────────────────
# Real Kafka backend (USE_REAL_KAFKA=true)
# ─────────────────────────────────────────────────────────────────────────────


def _get_real_producer() -> Any:
    """Instantiate a kafka-python KafkaProducer.

    Returns:
        Configured KafkaProducer instance.

    Raises:
        ImportError: If kafka-python is not installed.
    """
    from kafka import KafkaProducer  # type: ignore

    return KafkaProducer(
        bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )


def _get_real_consumer() -> Any:
    """Instantiate a kafka-python KafkaConsumer.

    Returns:
        Configured KafkaConsumer instance.
    """
    from kafka import KafkaConsumer  # type: ignore

    return KafkaConsumer(
        settings.KAFKA_TOPIC,
        bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
        group_id=settings.KAFKA_GROUP_ID,
        auto_offset_reset="earliest",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        consumer_timeout_ms=5000,
    )


# ─────────────────────────────────────────────────────────────────────────────
# Orchestration
# ─────────────────────────────────────────────────────────────────────────────


def produce(n: int) -> None:
    """Produce *n* synthetic order events to the configured backend.

    Args:
        n: Number of events to produce.
    """
    if settings.USE_REAL_KAFKA:
        producer = _get_real_producer()
        logger.info("Real Kafka producer connected to %s", settings.KAFKA_BOOTSTRAP_SERVERS)
        for i in range(n):
            event = _generate_order_event()
            producer.send(settings.KAFKA_TOPIC, event)
            logger.info("Produced event #%d: %s", i + 1, event["order_id"])
            time.sleep(settings.STREAMING_INTERVAL_SEC)
        producer.flush()
        producer.close()
    else:
        producer = LocalProducer(settings.KAFKA_TOPIC)
        logger.info("Local queue producer started (USE_REAL_KAFKA=false)")
        for i in range(n):
            event = _generate_order_event()
            producer.send(event)
            logger.info("Produced event #%d: %s", i + 1, event["order_id"])
            time.sleep(settings.STREAMING_INTERVAL_SEC)
        producer.close()


def consume() -> None:
    """Consume events from the configured backend and persist them."""
    if settings.USE_REAL_KAFKA:
        consumer = _get_real_consumer()
        records: list[dict[str, Any]] = []
        for message in consumer:
            records.append(message.value)
            logger.info("Consumed: %s", message.value.get("order_id"))
        consumer.close()
        if records:
            ts = datetime.utcnow().strftime("%Y%m%dT%H%M%S")
            out_file = _STREAMING_OUTPUT / f"stream_batch_{ts}.jsonl"
            with out_file.open("w") as fh:
                for r in records:
                    fh.write(json.dumps(r) + "\n")
            logger.info("Real consumer wrote %d records → %s", len(records), out_file)
    else:
        consumer = LocalConsumer(settings.KAFKA_TOPIC, settings.KAFKA_GROUP_ID)
        consumer.start(_STREAMING_OUTPUT, max_messages=settings.STREAMING_NUM_EVENTS)


def run() -> None:
    """Run producer and consumer concurrently in separate threads."""
    logger.info("=== Streaming simulation started (USE_REAL_KAFKA=%s) ===", settings.USE_REAL_KAFKA)

    producer_thread = threading.Thread(
        target=produce, args=(settings.STREAMING_NUM_EVENTS,), daemon=True
    )
    consumer_thread = threading.Thread(target=consume, daemon=False)

    consumer_thread.start()
    producer_thread.start()

    producer_thread.join()
    consumer_thread.join(timeout=30)

    logger.info("=== Streaming simulation complete ===")


if __name__ == "__main__":
    run()
