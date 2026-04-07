"""
pipeline/quality/quality_checks.py

Data quality validation layer.

Design decisions:
    - Each check is a pure function: takes DataFrames, returns a result dict.
    - Required columns are validated before running any check — cryptic
      KeyErrors deep in the pipeline are eliminated.
    - Referential integrity compares native types (int or str) without
      casting to string, so hashed string FKs and integer PKs are never
      falsely compared.
    - Reports are timestamped and appended to a JSONL archive (one JSON
      object per line) so quality history is preserved across runs.
    - Null FKs are counted and reported as a separate violation, not silently
      dropped.
"""

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

from config.settings import settings
from pipeline.utils.decorators import timed
from pipeline.utils.logging_config import get_logger

logger = get_logger(__name__, log_file=settings.LOGS_PATH / "quality.log")

_REPORT_PATH   = settings.PROCESSED_DATA_PATH / "quality_report.json"
_ARCHIVE_PATH  = settings.PROCESSED_DATA_PATH / "quality_report_archive.jsonl"


# ─────────────────────────────────────────────────────────────────────────────
# Guard: column existence
# ─────────────────────────────────────────────────────────────────────────────

def _require_columns(df: pd.DataFrame, columns: list[str], dataset: str) -> None:
    """Raise immediately if expected columns are missing.

    Args:
        df: DataFrame to inspect.
        columns: Expected column names.
        dataset: Dataset label for the error message.

    Raises:
        ValueError: Listing every missing column.
    """
    missing = [c for c in columns if c not in df.columns]
    if missing:
        raise ValueError(f"[{dataset}] Missing required columns: {missing}")


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
        dataset: Human-readable label.
        threshold: Max allowed null fraction [0, 1]. Defaults to settings value.

    Returns:
        Check result dict.
    """
    threshold = threshold if threshold is not None else settings.NULL_THRESHOLD
    null_fracs = df.isnull().mean()
    violations = {col: round(frac, 4) for col, frac in null_fracs.items() if frac > threshold}
    status = "PASS" if not violations else "FAIL"
    logger.info("null_rate check", extra={"dataset": dataset, "status": status,
                                          "violations": len(violations)})
    return _result("null_rate", dataset, status, len(violations), violations,
                   threshold=threshold)


def check_numeric_range(
    df: pd.DataFrame,
    dataset: str,
    column: str,
    min_val: float | None = None,
    max_val: float | None = None,
) -> dict[str, Any]:
    """Validate that numeric column values fall within [min_val, max_val].

    Args:
        df: DataFrame to inspect.
        dataset: Human-readable label.
        column: Column to validate.
        min_val: Minimum allowed value (inclusive).
        max_val: Maximum allowed value (inclusive).

    Returns:
        Check result dict.
    """
    _require_columns(df, [column], dataset)
    series = pd.to_numeric(df[column], errors="coerce")
    mask = pd.Series(True, index=series.index)
    if min_val is not None:
        mask &= series >= min_val
    if max_val is not None:
        mask &= series <= max_val
    n_violations = int((~mask | series.isna()).sum())
    status = "PASS" if n_violations == 0 else "FAIL"
    logger.info("range check", extra={"dataset": dataset, "column": column,
                                      "status": status, "violations": n_violations})
    return _result("numeric_range", dataset, status, n_violations, {},
                   column=column, min_val=min_val, max_val=max_val)


def check_referential_integrity(
    child_df: pd.DataFrame,
    parent_df: pd.DataFrame,
    fk_col: str,
    pk_col: str,
    child_name: str,
    parent_name: str,
) -> dict[str, Any]:
    """Validate that all non-null FK values exist in the parent PK set.

    Null FK values are counted separately as a distinct violation class —
    they are not silently ignored.

    Comparison is done in the native dtype of each column.  Hashed string
    FKs are compared to hashed string PKs; integer FKs to integer PKs.
    This prevents false mismatches caused by type coercion.

    Args:
        child_df: DataFrame with the foreign key column.
        parent_df: DataFrame with the primary key column.
        fk_col: Foreign key column name in child_df.
        pk_col: Primary key column name in parent_df.
        child_name: Label for child dataset (error messages).
        parent_name: Label for parent dataset (error messages).

    Returns:
        Check result dict.
    """
    _require_columns(child_df, [fk_col], child_name)
    _require_columns(parent_df, [pk_col], parent_name)

    fk_series = child_df[fk_col]
    null_count = int(fk_series.isna().sum())
    fk_values  = set(fk_series.dropna())
    pk_values  = set(parent_df[pk_col].dropna())
    orphans    = fk_values - pk_values
    n_errors   = len(orphans) + null_count
    status     = "PASS" if n_errors == 0 else "FAIL"

    logger.info(
        "RI check",
        extra={"child": child_name, "parent": parent_name, "fk_col": fk_col,
               "orphans": len(orphans), "null_fks": null_count, "status": status},
    )
    return _result(
        "referential_integrity", child_name, status, n_errors,
        {"sample_orphans": list(orphans)[:20], "null_fk_count": null_count},
        parent=parent_name, fk_col=fk_col, pk_col=pk_col,
    )


def check_duplicates(
    df: pd.DataFrame,
    dataset: str,
    pk_col: str,
) -> dict[str, Any]:
    """Validate uniqueness of a primary key column.

    Args:
        df: DataFrame to inspect.
        dataset: Human-readable label.
        pk_col: Column that must be unique.

    Returns:
        Check result dict.
    """
    _require_columns(df, [pk_col], dataset)
    dup_count = int(df[pk_col].duplicated().sum())
    status    = "PASS" if dup_count == 0 else "FAIL"
    logger.info("duplicate check", extra={"dataset": dataset, "pk_col": pk_col,
                                          "status": status, "duplicates": dup_count})
    return _result("duplicates", dataset, status, dup_count, {}, pk_col=pk_col)


def check_schema(
    df: pd.DataFrame,
    dataset: str,
    required_columns: list[str],
) -> dict[str, Any]:
    """Validate that all required columns are present.

    Args:
        df: DataFrame to inspect.
        dataset: Human-readable label.
        required_columns: Column names that must exist.

    Returns:
        Check result dict.
    """
    missing = [c for c in required_columns if c not in df.columns]
    status  = "PASS" if not missing else "FAIL"
    logger.info("schema check", extra={"dataset": dataset, "status": status,
                                       "missing": missing})
    return _result("schema", dataset, status, len(missing),
                   {"missing_columns": missing})


# ─────────────────────────────────────────────────────────────────────────────
# Result builder
# ─────────────────────────────────────────────────────────────────────────────

def _result(
    check: str,
    dataset: str,
    status: str,
    error_count: int,
    details: dict[str, Any],
    **extra: Any,
) -> dict[str, Any]:
    """Build a standardised check result dictionary.

    Args:
        check: Check type identifier.
        dataset: Dataset label.
        status: 'PASS' or 'FAIL'.
        error_count: Number of violations found.
        details: Arbitrary detail payload.
        **extra: Additional key-value pairs merged into the result.

    Returns:
        Standardised result dict.
    """
    return {
        "check":       check,
        "dataset":     dataset,
        "status":      status,
        "error_count": error_count,
        "details":     details,
        "checked_at":  datetime.utcnow().isoformat(),
        **extra,
    }


# ─────────────────────────────────────────────────────────────────────────────
# Report
# ─────────────────────────────────────────────────────────────────────────────

def write_report(results: list[dict[str, Any]]) -> None:
    """Serialise quality results to JSON and append to the JSONL archive.

    The latest run is always at quality_report.json (overwritten).
    Every run is appended to quality_report_archive.jsonl for trend analysis.

    Args:
        results: List of check result dicts.
    """
    _REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
    total  = len(results)
    failed = sum(1 for r in results if r["status"] == "FAIL")
    report = {
        "generated_at":   datetime.utcnow().isoformat(),
        "total_checks":   total,
        "passed":         total - failed,
        "failed":         failed,
        "overall_status": "PASS" if failed == 0 else "FAIL",
        "checks":         results,
    }

    # Latest report (overwritten)
    _REPORT_PATH.write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")

    # Append to archive (one JSON object per line)
    with _ARCHIVE_PATH.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps({
            "generated_at":   report["generated_at"],
            "overall_status": report["overall_status"],
            "passed":         report["passed"],
            "failed":         report["failed"],
        }) + "\n")

    logger.info(
        "Quality report written",
        extra={"path": str(_REPORT_PATH), "passed": total - failed, "total": total},
    )


# ─────────────────────────────────────────────────────────────────────────────
# Orchestration
# ─────────────────────────────────────────────────────────────────────────────

_EXPECTED_SCHEMAS = {
    "customers":   ["customer_id", "name", "email", "phone", "city", "state", "birth_date", "created_at"],
    "products":    ["product_id", "name", "category", "price", "cost"],
    "orders":      ["order_id", "customer_id", "order_date", "status", "total_amount"],
    "order_items": ["item_id", "order_id", "product_id", "quantity", "unit_price", "discount", "line_total"],
    "payments":    ["payment_id", "order_id", "method", "status", "amount"],
    "reviews":     ["review_id", "order_id", "customer_id", "product_id", "rating"],
}


@timed
def run() -> list[dict[str, Any]]:
    """Execute all quality checks and persist the report.

    Returns:
        List of individual check result dicts.
    """
    logger.info("=== Data quality checks started ===")
    raw = settings.RAW_DATA_PATH

    frames = {
        name: pd.read_csv(raw / f"{name}.csv")
        for name in _EXPECTED_SCHEMAS
    }

    results: list[dict[str, Any]] = []

    # 1. Schema presence checks — run first; stop on failure
    for name, df in frames.items():
        r = check_schema(df, name, _EXPECTED_SCHEMAS[name])
        results.append(r)
        if r["status"] == "FAIL":
            logger.error("Schema check failed for '%s' — aborting remaining checks", name)
            write_report(results)
            return results

    customers   = frames["customers"]
    products    = frames["products"]
    orders      = frames["orders"]
    items       = frames["order_items"]
    payments    = frames["payments"]
    reviews     = frames["reviews"]

    # 2. Null rates
    for name, df in frames.items():
        results.append(check_null_rate(df, name))

    # 3. Numeric ranges
    results += [
        check_numeric_range(products, "products",    "price",       min_val=0.01),
        check_numeric_range(products, "products",    "cost",        min_val=0),
        check_numeric_range(orders,   "orders",      "total_amount",min_val=0.01),
        check_numeric_range(items,    "order_items", "quantity",    min_val=1),
        check_numeric_range(items,    "order_items", "discount",    min_val=0,    max_val=1),
        check_numeric_range(reviews,  "reviews",     "rating",      min_val=1,    max_val=5),
    ]

    # 4. Referential integrity (raw, pre-hashing — integer PKs and FKs)
    results += [
        check_referential_integrity(orders,   customers, "customer_id", "customer_id", "orders",      "customers"),
        check_referential_integrity(items,    orders,    "order_id",    "order_id",    "order_items", "orders"),
        check_referential_integrity(items,    products,  "product_id",  "product_id",  "order_items", "products"),
        check_referential_integrity(payments, orders,    "order_id",    "order_id",    "payments",    "orders"),
        check_referential_integrity(reviews,  customers, "customer_id", "customer_id", "reviews",     "customers"),
    ]

    # 5. Duplicate PKs
    results += [
        check_duplicates(customers, "customers", "customer_id"),
        check_duplicates(products,  "products",  "product_id"),
        check_duplicates(orders,    "orders",    "order_id"),
        check_duplicates(payments,  "payments",  "payment_id"),
    ]

    write_report(results)
    logger.info("=== Data quality checks complete ===",
                extra={"passed": sum(1 for r in results if r["status"] == "PASS"),
                       "total": len(results)})
    return results


if __name__ == "__main__":
    run()
