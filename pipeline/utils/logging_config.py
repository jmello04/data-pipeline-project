"""
pipeline/utils/logging_config.py

Structured JSON logging configuration.

JSON logs are machine-readable and trivially queryable with tools like
jq, CloudWatch Insights, or Datadog.  Plain-text logs go to stdout.

Usage:
    from pipeline.utils.logging_config import get_logger
    logger = get_logger(__name__, log_file=settings.LOGS_PATH / "stage.log")
    logger.info("rows loaded", extra={"rows": 5000, "source": "orders.csv"})
"""

import logging
import sys
from pathlib import Path
from pythonjsonlogger import jsonlogger


def get_logger(name: str, log_file: Path | None = None, level: int = logging.INFO) -> logging.Logger:
    """Build a logger that writes JSON to a file and plain text to stdout.

    Args:
        name: Logger name (typically __name__).
        log_file: Optional path for the JSON log file.  Parent directory
            is created automatically.
        level: Logging level (default INFO).

    Returns:
        Configured Logger instance.
    """
    logger = logging.getLogger(name)

    if logger.handlers:
        return logger  # already configured — avoid duplicate handlers

    logger.setLevel(level)

    # ── JSON file handler ────────────────────────────────────────────────
    if log_file is not None:
        log_file.parent.mkdir(parents=True, exist_ok=True)
        json_formatter = jsonlogger.JsonFormatter(
            fmt="%(asctime)s %(name)s %(levelname)s %(message)s",
            datefmt="%Y-%m-%dT%H:%M:%SZ",
            rename_fields={"asctime": "ts", "levelname": "level", "name": "logger"},
        )
        file_handler = logging.FileHandler(log_file, encoding="utf-8")
        file_handler.setFormatter(json_formatter)
        logger.addHandler(file_handler)

    # ── Plain-text stdout handler ────────────────────────────────────────
    console_formatter = logging.Formatter(
        fmt="%(asctime)s [%(levelname)s] %(name)s – %(message)s",
        datefmt="%H:%M:%S",
    )
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(console_formatter)
    logger.addHandler(console_handler)

    logger.propagate = False
    return logger
