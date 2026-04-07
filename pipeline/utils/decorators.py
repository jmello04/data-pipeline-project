"""
pipeline/utils/decorators.py

Reusable function decorators for resilience and observability.

Usage:
    from pipeline.utils.decorators import retry, timed

    @retry(max_attempts=3, exceptions=(IOError, OSError))
    @timed
    def read_parquet(path: Path) -> pd.DataFrame:
        ...
"""

import functools
import logging
import time
from collections.abc import Callable
from typing import TypeVar

_F = TypeVar("_F", bound=Callable)
_logger = logging.getLogger(__name__)


def retry(
    max_attempts: int = 3,
    backoff_factor: float = 2.0,
    exceptions: tuple[type[Exception], ...] = (Exception,),
) -> Callable[[_F], _F]:
    """Retry a function with exponential backoff on specified exceptions.

    Args:
        max_attempts: Total number of attempts before re-raising.
        backoff_factor: Multiplier for sleep duration between retries.
            Attempt k waits backoff_factor^(k-1) seconds.
        exceptions: Tuple of exception types that trigger a retry.

    Returns:
        Decorator that wraps the target function.

    Example:
        @retry(max_attempts=5, exceptions=(ConnectionError,))
        def connect_to_kafka() -> None: ...
    """
    def decorator(fn: _F) -> _F:
        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            for attempt in range(1, max_attempts + 1):
                try:
                    return fn(*args, **kwargs)
                except exceptions as exc:
                    if attempt == max_attempts:
                        _logger.error(
                            "All %d attempts failed for '%s': %s",
                            max_attempts, fn.__qualname__, exc,
                        )
                        raise
                    wait = backoff_factor ** (attempt - 1)
                    _logger.warning(
                        "Attempt %d/%d failed for '%s': %s – retrying in %.1fs",
                        attempt, max_attempts, fn.__qualname__, exc, wait,
                    )
                    time.sleep(wait)
        return wrapper  # type: ignore[return-value]
    return decorator


def timed(fn: _F) -> _F:
    """Log the wall-clock execution time of a function.

    Uses the decorated function's own module logger so the log record carries
    the correct logger name and inherits any handlers (JSON file, stdout)
    that the module configured via get_logger().  The logger is resolved at
    call time (inside wrapper), not at decoration time, so it is always the
    fully-configured instance even if get_logger() was called after @timed
    was applied.

    Args:
        fn: Function to instrument.

    Returns:
        Wrapped function that logs elapsed time at INFO level.
    """
    @functools.wraps(fn)
    def wrapper(*args, **kwargs):
        log = logging.getLogger(fn.__module__)
        start = time.perf_counter()
        try:
            result = fn(*args, **kwargs)
            elapsed = time.perf_counter() - start
            log.info(
                "finished",
                extra={"fn": fn.__qualname__, "elapsed_s": round(elapsed, 3)},
            )
            return result
        except Exception:
            elapsed = time.perf_counter() - start
            log.error(
                "raised",
                extra={"fn": fn.__qualname__, "elapsed_s": round(elapsed, 3)},
            )
            raise
    return wrapper  # type: ignore[return-value]
