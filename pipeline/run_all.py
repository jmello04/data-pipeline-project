"""
pipeline/run_all.py

Pipeline orchestrator — runs all stages in dependency order.

Usage:
    python pipeline/run_all.py

Each stage is wrapped with:
    - structured logging (start / end / elapsed)
    - exception capture that logs the traceback and raises immediately
    - no silently-swallowed failures
"""

import logging
import sys
import time
from dataclasses import dataclass, field
from typing import Callable

from config.settings import settings
from pipeline.utils.logging_config import get_logger

logger = get_logger(__name__, log_file=settings.LOGS_PATH / "pipeline.log")


# ─────────────────────────────────────────────────────────────────────────────
# Stage registry
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class Stage:
    """A single pipeline stage.

    Attributes:
        name: Human-readable label shown in logs.
        fn: Zero-argument callable that executes the stage.
        depends_on: Names of stages that must succeed before this one runs.
    """
    name:       str
    fn:         Callable
    depends_on: list[str] = field(default_factory=list)


def _build_stages() -> list[Stage]:
    """Import and register all pipeline stages.

    Imports are deferred here so import errors surface as a named stage
    failure rather than a silent crash at module load time.

    Returns:
        Ordered list of Stage objects.
    """
    from pipeline.ingestion.ingest            import run as ingest_run
    from pipeline.quality.quality_checks      import run as quality_run
    from pipeline.transformation.transform    import run as transform_run
    from pipeline.warehouse.dw_model          import run as warehouse_run
    from pipeline.streaming.kafka_simulation  import run as streaming_run

    return [
        Stage("bronze_ingestion",       ingest_run),
        Stage("quality_checks",         quality_run,    depends_on=["bronze_ingestion"]),
        Stage("silver_transformation",  transform_run,  depends_on=["bronze_ingestion"]),
        Stage("gold_warehouse",         warehouse_run,  depends_on=["silver_transformation"]),
        Stage("streaming_simulation",   streaming_run),
    ]


# ─────────────────────────────────────────────────────────────────────────────
# Runner
# ─────────────────────────────────────────────────────────────────────────────

def _run_stage(stage: Stage, completed: set[str]) -> float:
    """Execute one stage, enforcing dependency completion.

    Args:
        stage: Stage to execute.
        completed: Set of successfully completed stage names.

    Returns:
        Elapsed wall-clock time in seconds.

    Raises:
        RuntimeError: If a dependency has not completed successfully.
        Exception: Re-raises any exception from the stage function.
    """
    missing_deps = [d for d in stage.depends_on if d not in completed]
    if missing_deps:
        raise RuntimeError(
            f"Stage '{stage.name}' cannot run: dependencies not met: {missing_deps}"
        )

    logger.info("Stage starting", extra={"stage": stage.name})
    start = time.perf_counter()
    try:
        stage.fn()
    except Exception:
        elapsed = time.perf_counter() - start
        logger.exception("Stage FAILED", extra={"stage": stage.name, "elapsed_s": round(elapsed, 2)})
        raise
    elapsed = time.perf_counter() - start
    logger.info("Stage complete", extra={"stage": stage.name, "elapsed_s": round(elapsed, 2)})
    return elapsed


def main() -> None:
    """Run all pipeline stages, logging a summary on completion."""
    logger.info(
        "Pipeline starting",
        extra={
            "num_customers": settings.NUM_CUSTOMERS,
            "num_products":  settings.NUM_PRODUCTS,
            "num_orders":    settings.NUM_ORDERS,
        },
    )

    stages    = _build_stages()
    completed: set[str] = set()
    timings:   dict[str, float] = {}
    total_start = time.perf_counter()

    for stage in stages:
        try:
            elapsed = _run_stage(stage, completed)
            completed.add(stage.name)
            timings[stage.name] = elapsed
        except Exception:
            failed_after = time.perf_counter() - total_start
            logger.error(
                "Pipeline aborted",
                extra={"failed_stage": stage.name, "total_elapsed_s": round(failed_after, 2)},
            )
            sys.exit(1)

    total_elapsed = time.perf_counter() - total_start
    logger.info(
        "Pipeline complete",
        extra={"total_elapsed_s": round(total_elapsed, 2), "stage_timings": timings},
    )
    for stage_name, elapsed in timings.items():
        logger.info("Stage timing", extra={"stage": stage_name, "elapsed_s": round(elapsed, 2)})


if __name__ == "__main__":
    main()
