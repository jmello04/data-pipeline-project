"""
pipeline/utils/logging_config.py

Structured JSON logging configuration.

When python-json-logger is installed (production), log files are written in
JSON for machine-readable querying with jq, CloudWatch Insights, or Datadog.
When it is absent (e.g. lightweight test environments), the file handler falls
back to the same plain-text format used by stdout — no code change required.

Usage:
    from pipeline.utils.logging_config import get_logger
    logger = get_logger(__name__, log_file=settings.LOGS_PATH / "stage.log")
    logger.info("rows loaded", extra={"rows": 5000, "source": "orders.csv"})
"""

import logging
import sys
from pathlib import Path

try:
    from pythonjsonlogger import jsonlogger as _jsonlogger
    _JSON_AVAILABLE = True
except ImportError:  # graceful degradation — tests run without the package
    _JSON_AVAILABLE = False


def get_logger(name: str, log_file: Path | None = None, level: int = logging.INFO) -> logging.Logger:
    """Build a logger that writes to a file and plain text to stdout.

    When python-json-logger is available the file handler uses JSON format.
    When it is not available the file handler uses the same plain-text format
    as stdout.  Either way every log call behaves identically for callers.

    Args:
        name: Logger name (typically __name__).
        log_file: Optional path for the log file.  Parent directory is
            created automatically.
        level: Logging level (default INFO).

    Returns:
        Configured Logger instance.  A second call with the same name returns
        the already-configured instance unchanged.
    """
    logger = logging.getLogger(name)

    if logger.handlers:
        return logger  # already configured — avoid duplicate handlers

    logger.setLevel(level)

    _plain_fmt = logging.Formatter(
        fmt="%(asctime)s [%(levelname)s] %(name)s – %(message)s",
        datefmt="%H:%M:%S",
    )

    # ── File handler (JSON when available, plain-text otherwise) ─────────
    if log_file is not None:
        log_file.parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(log_file, encoding="utf-8")

        if _JSON_AVAILABLE:
            file_handler.setFormatter(
                _jsonlogger.JsonFormatter(
                    fmt="%(asctime)s %(name)s %(levelname)s %(message)s",
                    datefmt="%Y-%m-%dT%H:%M:%SZ",
                    rename_fields={
                        "asctime":   "ts",
                        "levelname": "level",
                        "name":      "logger",
                    },
                )
            )
        else:
            file_handler.setFormatter(_plain_fmt)

        logger.addHandler(file_handler)

    # ── stdout handler (always plain-text) ───────────────────────────────
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(_plain_fmt)
    logger.addHandler(console_handler)

    logger.propagate = False
    return logger
