"""
pipeline/run_all.py

Pipeline orchestrator – executes all stages in order with structured logging.

Usage:
    python pipeline/run_all.py
"""

import logging
import sys
import time
from pathlib import Path

from config.settings import settings

# ── Root logger ────────────────────────────────────────────────────────────────
settings.LOGS_PATH.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s – %(message)s",
    handlers=[
        logging.FileHandler(settings.LOGS_PATH / "pipeline.log"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger("run_all")


def _run_stage(label: str, fn: "callable") -> None:  # type: ignore[type-arg]
    """Execute a pipeline stage and log elapsed time.

    Args:
        label: Human-readable stage name.
        fn: Zero-argument callable that executes the stage.
    """
    logger.info("━━━ Stage: %-35s ━━━ START", label)
    start = time.perf_counter()
    try:
        fn()
    except Exception:
        logger.exception("Stage '%s' failed", label)
        raise
    elapsed = time.perf_counter() - start
    logger.info("━━━ Stage: %-35s ━━━ DONE (%.2fs)", label, elapsed)


def main() -> None:
    """Run all pipeline stages sequentially."""
    logger.info("╔══════════════════════════════════════════╗")
    logger.info("║     DATA PIPELINE — full run             ║")
    logger.info("╚══════════════════════════════════════════╝")

    from pipeline.ingestion.ingest import run as ingest_run
    from pipeline.quality.quality_checks import run as quality_run
    from pipeline.transformation.transform import run as transform_run
    from pipeline.warehouse.dw_model import run as warehouse_run
    from pipeline.streaming.kafka_simulation import run as streaming_run

    stages = [
        ("Bronze – Ingestion", ingest_run),
        ("Quality Checks", quality_run),
        ("Silver – Transformation", transform_run),
        ("Gold – Dimensional Model", warehouse_run),
        ("Streaming Simulation", streaming_run),
    ]

    total_start = time.perf_counter()
    for label, fn in stages:
        _run_stage(label, fn)

    total = time.perf_counter() - total_start
    logger.info("Pipeline complete in %.2fs", total)


if __name__ == "__main__":
    main()
