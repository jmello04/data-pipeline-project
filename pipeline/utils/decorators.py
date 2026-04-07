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

    Args:
        fn: Function to instrument.

    Returns:
        Wrapped function that logs elapsed time at INFO level.
    """
    @functools.wraps(fn)
    def wrapper(*args, **kwargs):
        start = time.perf_counter()
        try:
            result = fn(*args, **kwargs)
            elapsed = time.perf_counter() - start
            logging.getLogger(fn.__module__).info(
                "%s finished in %.3fs", fn.__qualname__, elapsed,
                extra={"fn": fn.__qualname__, "elapsed_s": round(elapsed, 3)},
            )
            return result
        except Exception:
            elapsed = time.perf_counter() - start
            logging.getLogger(fn.__module__).error(
                "%s raised after %.3fs", fn.__qualname__, elapsed,
                extra={"fn": fn.__qualname__, "elapsed_s": round(elapsed, 3)},
            )
            raise
    return wrapper  # type: ignore[return-value]
