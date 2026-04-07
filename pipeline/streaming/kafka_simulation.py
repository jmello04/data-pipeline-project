"""
pipeline/streaming/kafka_simulation.py

Streaming layer: Kafka producer/consumer simulation.

Design decisions:
    - The local queue backend is encapsulated in a class with a reset()
      method so tests can clear state between runs.
    - Producer and consumer share a threading.Event for handshaking:
      consumer signals "ready" before producer starts emitting.  This
      eliminates the race where the producer drains the queue before the
      consumer has started.
    - JSONL output is written atomically (write to temp file → rename).
      A crash mid-write leaves the previous file intact.
    - Faker is re-seeded inside produce() so repeated calls are reproducible.
"""

import json
import logging
import queue
import tempfile
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import Any

from faker import Faker

from config.settings import settings
from pipeline.utils.decorators import retry
from pipeline.utils.logging_config import get_logger

logger = get_logger(__name__, log_file=settings.LOGS_PATH / "streaming.log")

fake = Faker("pt_BR")

_STREAMING_OUTPUT = settings.PROCESSED_DATA_PATH / "streaming"
_STREAMING_OUTPUT.mkdir(parents=True, exist_ok=True)


# ─────────────────────────────────────────────────────────────────────────────
# Event schema
# ─────────────────────────────────────────────────────────────────────────────

def _order_event() -> dict[str, Any]:
    """Generate a single synthetic order-event payload.

    Returns:
        Dict representing one order event.
    """
    # order_id uses a high integer range (100 M+) to avoid collision with
    # batch-ingested orders (1 – NUM_ORDERS).  Both are integers so the
    # nk_order_id column stays type-consistent across batch and streaming.
    return {
        "order_id":    fake.random_int(min=100_000_000, max=999_999_999),
        "customer_id": fake.random_int(min=1, max=settings.NUM_CUSTOMERS),
        "product_id":  fake.random_int(min=1, max=settings.NUM_PRODUCTS),
        "quantity":    fake.random_int(min=1, max=5),
        "unit_price":  round(fake.random_int(min=500, max=150_000) / 100, 2),
        "channel":     fake.random_element(["web", "mobile", "marketplace"]),
        "event_time":  datetime.utcnow().isoformat(),
    }


# ─────────────────────────────────────────────────────────────────────────────
# Local queue backend
# ─────────────────────────────────────────────────────────────────────────────

class _LocalBus:
    """Thread-safe in-process message bus backed by a queue.Queue.

    Using a class (instead of a module-level queue) ensures that each
    test or simulation run can start from a clean state by calling reset().
    """

    def __init__(self) -> None:
        self._q: queue.Queue[str] = queue.Queue()

    def put(self, payload: dict[str, Any]) -> None:
        """Serialise and enqueue a message.

        Args:
            payload: Event dict to enqueue.
        """
        self._q.put(json.dumps(payload))

    def get(self, timeout: float = 2.0) -> dict[str, Any] | None:
        """Dequeue and deserialise one message.

        Args:
            timeout: Seconds to wait before returning None.

        Returns:
            Deserialised event dict, or None if the queue is empty.
        """
        try:
            raw = self._q.get(timeout=timeout)
            self._q.task_done()
            return json.loads(raw)
        except queue.Empty:
            return None

    def reset(self) -> None:
        """Drain the queue (useful between test runs)."""
        while not self._q.empty():
            try:
                self._q.get_nowait()
                self._q.task_done()
            except queue.Empty:
                break

    @property
    def qsize(self) -> int:
        """Current approximate queue depth."""
        return self._q.qsize()


_bus = _LocalBus()  # singleton shared between producer and consumer threads


# ─────────────────────────────────────────────────────────────────────────────
# Local producer / consumer
# ─────────────────────────────────────────────────────────────────────────────

def _produce_local(n: int, ready: threading.Event) -> None:
    """Produce *n* events to the local bus after consumer signals ready.

    Args:
        n: Number of events to produce.
        ready: Event set by the consumer when it is ready to receive.
    """
    ready.wait(timeout=10)  # wait for consumer to start
    logger.info("LocalProducer: consumer ready, starting", extra={"n": n})
    Faker.seed(settings.FAKE_DATA_SEED)  # reproducible across repeated calls
    for i in range(1, n + 1):
        event = _order_event()
        _bus.put(event)
        logger.debug("Produced event %d/%d: %s", i, n, event["order_id"])
        time.sleep(settings.STREAMING_INTERVAL_SEC)
    logger.info("LocalProducer: finished", extra={"produced": n})


def _consume_local(
    output_path: Path,
    max_messages: int,
    ready: threading.Event,
) -> None:
    """Consume up to *max_messages* events and write them atomically to JSONL.

    Args:
        output_path: Directory for JSONL output files.
        max_messages: Stop after this many messages.
        ready: Event to set once the consumer is initialised.
    """
    output_path.mkdir(parents=True, exist_ok=True)
    records: list[dict[str, Any]] = []

    ready.set()  # signal producer that we are ready
    logger.info("LocalConsumer: started, waiting for events")

    consumed = 0
    while consumed < max_messages:
        event = _bus.get(timeout=3)
        if event is None:
            if consumed > 0:
                break  # queue drained
            continue
        records.append(event)
        consumed += 1
        logger.debug("Consumed event %d/%d: %s", consumed, max_messages, event.get("order_id"))

    if records:
        ts = datetime.utcnow().strftime("%Y%m%dT%H%M%S")
        out_file = output_path / f"stream_batch_{ts}.jsonl"
        _write_jsonl_atomic(records, out_file)
        logger.info("LocalConsumer: wrote batch", extra={"records": len(records),
                                                          "file": str(out_file)})
    else:
        logger.warning("LocalConsumer: no events received")


# ─────────────────────────────────────────────────────────────────────────────
# Atomic JSONL writer
# ─────────────────────────────────────────────────────────────────────────────

def _write_jsonl_atomic(records: list[dict[str, Any]], out_file: Path) -> None:
    """Write *records* to a JSONL file atomically (temp file → rename).

    If the process crashes mid-write, the previous file at *out_file* is
    unaffected.

    Args:
        records: List of dicts to serialise.
        out_file: Target file path.
    """
    dir_ = out_file.parent
    dir_.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile("w", dir=dir_, delete=False, suffix=".jsonl.tmp",
                                     encoding="utf-8") as tmp:
        for rec in records:
            tmp.write(json.dumps(rec) + "\n")
        tmp_path = Path(tmp.name)
    tmp_path.replace(out_file)


# ─────────────────────────────────────────────────────────────────────────────
# Real Kafka backend
# ─────────────────────────────────────────────────────────────────────────────

@retry(max_attempts=3, exceptions=(Exception,))
def _get_real_producer() -> Any:
    """Instantiate a kafka-python KafkaProducer.

    Returns:
        Configured KafkaProducer.
    """
    from kafka import KafkaProducer  # type: ignore
    return KafkaProducer(
        bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",          # wait for all in-sync replicas
        retries=3,
    )


@retry(max_attempts=3, exceptions=(Exception,))
def _get_real_consumer() -> Any:
    """Instantiate a kafka-python KafkaConsumer.

    Returns:
        Configured KafkaConsumer.
    """
    from kafka import KafkaConsumer  # type: ignore
    return KafkaConsumer(
        settings.KAFKA_TOPIC,
        bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
        group_id=settings.KAFKA_GROUP_ID,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        consumer_timeout_ms=5000,
    )


# ─────────────────────────────────────────────────────────────────────────────
# Orchestration
# ─────────────────────────────────────────────────────────────────────────────

def run() -> None:
    """Run producer and consumer concurrently.

    Uses a threading.Event handshake so the producer never emits before the
    consumer is ready — eliminating the startup race condition.
    """
    logger.info("=== Streaming started ===",
                extra={"backend": "kafka" if settings.USE_REAL_KAFKA else "local_queue"})
    n = settings.STREAMING_NUM_EVENTS

    if settings.USE_REAL_KAFKA:
        # Real Kafka: producer and consumer run sequentially for simplicity
        producer = _get_real_producer()
        Faker.seed(settings.FAKE_DATA_SEED)
        for i in range(n):
            producer.send(settings.KAFKA_TOPIC, _order_event())
            time.sleep(settings.STREAMING_INTERVAL_SEC)
        producer.flush()
        producer.close()

        consumer = _get_real_consumer()
        records = [msg.value for msg in consumer]
        consumer.close()

        if records:
            ts = datetime.utcnow().strftime("%Y%m%dT%H%M%S")
            _write_jsonl_atomic(records, _STREAMING_OUTPUT / f"stream_batch_{ts}.jsonl")
    else:
        _bus.reset()
        ready = threading.Event()

        producer_t = threading.Thread(
            target=_produce_local,
            args=(n, ready),
            daemon=True,
            name="LocalProducer",
        )
        consumer_t = threading.Thread(
            target=_consume_local,
            args=(_STREAMING_OUTPUT, n, ready),
            daemon=False,
            name="LocalConsumer",
        )

        consumer_t.start()
        producer_t.start()
        producer_t.join()
        consumer_t.join(timeout=60)

    logger.info("=== Streaming complete ===")


if __name__ == "__main__":
    run()
