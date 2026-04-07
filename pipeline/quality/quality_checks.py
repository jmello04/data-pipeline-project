"""
pipeline/quality/quality_checks.py

Data quality validation layer.

Checks performed:
    - Null rate per column vs. configurable threshold.
    - Numeric range validation.
    - Referential integrity (FK ⊆ PK).
    - Duplicate primary-key detection.

Results are written to data/quality_report.json with PASS/FAIL status,
error counts, and a UTC timestamp.
"""

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

from config.settings import settings

# ── Logging ────────────────────────────────────────────────────────────────────
settings.LOGS_PATH.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s – %(message)s",
    handlers=[
        logging.FileHandler(settings.LOGS_PATH / "quality.log"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)

_REPORT_PATH = settings.PROCESSED_DATA_PATH / "quality_report.json"


# ─────────────────────────────────────────────────────────────────────────────
# Individual checks
# ─────────────────────────────────────────────────────────────────────────────


def check_null_rate(
    df: pd.DataFrame,
    dataset: str,
    threshold: float | None = None,
) -> dict[str, Any]:
    """Validate that no column exceeds the permitted null rate.

    Args:
        df: DataFrame to inspect.
        dataset: Human-readable label used in the report.
        threshold: Maximum allowed fraction of nulls (0–1).  Defaults to
            settings.NULL_THRESHOLD.

    Returns:
        Check result dict with keys: check, dataset, status, errors,
        details, checked_at.
    """
    threshold = threshold if threshold is not None else settings.NULL_THRESHOLD
    null_fractions = df.isnull().mean()
    violations = null_fractions[null_fractions > threshold].to_dict()
    status = "PASS" if not violations else "FAIL"
    logger.info("[%s] null_rate check → %s (violations: %s)", dataset, status, violations)
    return {
        "check": "null_rate",
        "dataset": dataset,
        "threshold": threshold,
        "status": status,
        "error_count": len(violations),
        "details": violations,
        "checked_at": datetime.utcnow().isoformat(),
    }


def check_numeric_range(
    df: pd.DataFrame,
    dataset: str,
    column: str,
    min_val: float | None = None,
    max_val: float | None = None,
) -> dict[str, Any]:
    """Validate that numeric values fall within [min_val, max_val].

    Args:
        df: DataFrame to inspect.
        dataset: Human-readable label.
        column: Column name to validate.
        min_val: Minimum allowed value (inclusive).  None = no lower bound.
        max_val: Maximum allowed value (inclusive).  None = no upper bound.

    Returns:
        Check result dict.
    """
    series = pd.to_numeric(df[column], errors="coerce")
    mask = pd.Series([True] * len(series), index=series.index)
    if min_val is not None:
        mask &= series >= min_val
    if max_val is not None:
        mask &= series <= max_val
    out_of_range = int((~mask).sum())
    status = "PASS" if out_of_range == 0 else "FAIL"
    logger.info(
        "[%s] range check on '%s' [%s, %s] → %s (%d violations)",
        dataset, column, min_val, max_val, status, out_of_range,
    )
    return {
        "check": "numeric_range",
        "dataset": dataset,
        "column": column,
        "min_val": min_val,
        "max_val": max_val,
        "status": status,
        "error_count": out_of_range,
        "details": {},
        "checked_at": datetime.utcnow().isoformat(),
    }


def check_referential_integrity(
    child_df: pd.DataFrame,
    parent_df: pd.DataFrame,
    fk_col: str,
    pk_col: str,
    child_name: str,
    parent_name: str,
) -> dict[str, Any]:
    """Validate that all FK values exist in the parent PK set.

    Args:
        child_df: DataFrame containing the foreign key column.
        parent_df: DataFrame containing the primary key column.
        fk_col: Foreign key column name in child_df.
        pk_col: Primary key column name in parent_df.
        child_name: Label for child dataset.
        parent_name: Label for parent dataset.

    Returns:
        Check result dict.
    """
    fk_values = set(child_df[fk_col].dropna().astype(str))
    pk_values = set(parent_df[pk_col].dropna().astype(str))
    orphans = fk_values - pk_values
    status = "PASS" if not orphans else "FAIL"
    logger.info(
        "RI [%s.%s → %s.%s] → %s (%d orphan keys)",
        child_name, fk_col, parent_name, pk_col, status, len(orphans),
    )
    return {
        "check": "referential_integrity",
        "child": child_name,
        "parent": parent_name,
        "fk_col": fk_col,
        "pk_col": pk_col,
        "status": status,
        "error_count": len(orphans),
        "details": {"sample_orphans": list(orphans)[:10]},
        "checked_at": datetime.utcnow().isoformat(),
    }


def check_duplicates(
    df: pd.DataFrame,
    dataset: str,
    pk_col: str,
) -> dict[str, Any]:
    """Validate uniqueness of the primary key column.

    Args:
        df: DataFrame to inspect.
        dataset: Human-readable label.
        pk_col: Column that should be unique.

    Returns:
        Check result dict.
    """
    dup_count = int(df[pk_col].duplicated().sum())
    status = "PASS" if dup_count == 0 else "FAIL"
    logger.info("[%s] duplicate check on '%s' → %s (%d dupes)", dataset, pk_col, status, dup_count)
    return {
        "check": "duplicates",
        "dataset": dataset,
        "pk_col": pk_col,
        "status": status,
        "error_count": dup_count,
        "details": {},
        "checked_at": datetime.utcnow().isoformat(),
    }


# ─────────────────────────────────────────────────────────────────────────────
# Report
# ─────────────────────────────────────────────────────────────────────────────


def write_report(results: list[dict[str, Any]]) -> None:
    """Serialise quality check results to JSON.

    Args:
        results: List of check result dicts produced by individual checks.
    """
    _REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
    total = len(results)
    failed = sum(1 for r in results if r["status"] == "FAIL")
    report = {
        "generated_at": datetime.utcnow().isoformat(),
        "total_checks": total,
        "passed": total - failed,
        "failed": failed,
        "overall_status": "PASS" if failed == 0 else "FAIL",
        "checks": results,
    }
    _REPORT_PATH.write_text(json.dumps(report, indent=2, default=str))
    logger.info(
        "Quality report written → %s [%d/%d PASS]", _REPORT_PATH, total - failed, total
    )


# ─────────────────────────────────────────────────────────────────────────────
# Orchestration
# ─────────────────────────────────────────────────────────────────────────────


def run() -> list[dict[str, Any]]:
    """Execute all quality checks and persist the report.

    Returns:
        List of individual check result dicts.
    """
    logger.info("=== Data quality checks started ===")

    raw = settings.RAW_DATA_PATH
    customers = pd.read_csv(raw / "customers.csv")
    products = pd.read_csv(raw / "products.csv")
    orders = pd.read_csv(raw / "orders.csv")
    items = pd.read_csv(raw / "order_items.csv")
    payments = pd.read_csv(raw / "payments.csv")
    reviews = pd.read_csv(raw / "reviews.csv")

    results: list[dict[str, Any]] = []

    # ── Null rate ────────────────────────────────────────────────────────────
    for name, df in [
        ("customers", customers),
        ("products", products),
        ("orders", orders),
        ("order_items", items),
        ("payments", payments),
        ("reviews", reviews),
    ]:
        results.append(check_null_rate(df, name))

    # ── Numeric ranges ───────────────────────────────────────────────────────
    results.append(check_numeric_range(products, "products", "price", min_val=0))
    results.append(check_numeric_range(products, "products", "cost", min_val=0))
    results.append(check_numeric_range(orders, "orders", "total_amount", min_val=0))
    results.append(check_numeric_range(items, "order_items", "quantity", min_val=1))
    results.append(check_numeric_range(items, "order_items", "discount", min_val=0, max_val=1))
    results.append(check_numeric_range(reviews, "reviews", "rating", min_val=1, max_val=5))

    # ── Referential integrity ────────────────────────────────────────────────
    results.append(
        check_referential_integrity(orders, customers, "customer_id", "customer_id", "orders", "customers")
    )
    results.append(
        check_referential_integrity(items, orders, "order_id", "order_id", "order_items", "orders")
    )
    results.append(
        check_referential_integrity(items, products, "product_id", "product_id", "order_items", "products")
    )
    results.append(
        check_referential_integrity(payments, orders, "order_id", "order_id", "payments", "orders")
    )
    results.append(
        check_referential_integrity(reviews, customers, "customer_id", "customer_id", "reviews", "customers")
    )

    # ── Duplicates ───────────────────────────────────────────────────────────
    results.append(check_duplicates(customers, "customers", "customer_id"))
    results.append(check_duplicates(products, "products", "product_id"))
    results.append(check_duplicates(orders, "orders", "order_id"))
    results.append(check_duplicates(payments, "payments", "payment_id"))

    write_report(results)
    logger.info("=== Data quality checks complete ===")
    return results


if __name__ == "__main__":
    run()
