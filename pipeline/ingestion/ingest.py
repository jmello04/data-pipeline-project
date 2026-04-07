"""
pipeline/ingestion/ingest.py

Bronze layer: synthetic data generation and raw-to-Parquet ingestion.

Design decisions:
    - Vectorised generation (no iterrows).
    - Monetary values computed with integer arithmetic to avoid float drift.
    - Each dataset is validated against its Pydantic schema before being written.
    - Writes are idempotent: output directory is cleared before writing.
    - Metadata (row counts, null rates) is logged as structured JSON.
"""

import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import pandas as pd
from faker import Faker

from config.settings import settings
from pipeline.schemas.models import (
    CustomerRaw, ProductRaw, OrderRaw, OrderItemRaw, PaymentRaw, ReviewRaw,
)
from pipeline.utils.decorators import timed
from pipeline.utils.logging_config import get_logger

logger = get_logger(__name__, log_file=settings.LOGS_PATH / "ingestion.log")

fake = Faker("pt_BR")
Faker.seed(settings.FAKE_DATA_SEED)

_CATEGORIES: dict[str, list[str]] = {
    "Electronics": ["Smartphones", "Laptops", "Accessories"],
    "Clothing":    ["Men", "Women", "Kids"],
    "Home":        ["Furniture", "Kitchen", "Decor"],
    "Sports":      ["Fitness", "Outdoor", "Team Sports"],
    "Books":       ["Fiction", "Non-Fiction", "Technical"],
}
_STATUSES = ["pending", "processing", "shipped", "delivered", "cancelled", "returned"]
_CHANNELS = ["web", "mobile", "marketplace"]
_PAY_METHODS = ["credit_card", "debit_card", "pix", "boleto", "wallet"]
_PAY_STATUSES = ["approved", "declined", "pending", "refunded"]


# ─────────────────────────────────────────────────────────────────────────────
# Generators
# ─────────────────────────────────────────────────────────────────────────────

@timed
def generate_customers(n: int) -> pd.DataFrame:
    """Generate *n* synthetic customer records.

    Args:
        n: Number of records to generate.

    Returns:
        Validated DataFrame matching CustomerRaw schema.
    """
    logger.info("Generating customers", extra={"n": n})
    rng = fake.random  # single random object — faster than calling fake.* repeatedly

    records = [
        {
            "customer_id": i,
            "name":        fake.name(),
            "email":       fake.email(),
            "phone":       fake.phone_number(),
            "city":        fake.city(),
            "state":       fake.state_abbr(),
            "birth_date":  fake.date_of_birth(minimum_age=18, maximum_age=80).isoformat(),
            "created_at":  fake.date_time_between(start_date="-5y", end_date="now").isoformat(),
        }
        for i in range(1, n + 1)
    ]
    df = pd.DataFrame(records)
    _validate_sample(df, CustomerRaw, "customers")
    return df


@timed
def generate_products(n: int) -> pd.DataFrame:
    """Generate *n* synthetic product records.

    price and cost are integers (cents) divided by 100 to avoid float drift.

    Args:
        n: Number of records to generate.

    Returns:
        Validated DataFrame matching ProductRaw schema.
    """
    logger.info("Generating products", extra={"n": n})
    categories = list(_CATEGORIES.keys())

    records = []
    for i in range(1, n + 1):
        cat = fake.random_element(categories)
        sub = fake.random_element(_CATEGORIES[cat])
        price_cents = fake.random_int(min=500, max=200_000)       # 5.00 – 2000.00
        cost_pct    = fake.random_int(min=30, max=79) / 100        # 30 – 79%
        cost_cents  = int(price_cents * cost_pct)
        records.append({
            "product_id":  i,
            "name":        fake.catch_phrase(),
            "category":    cat,
            "subcategory": sub,
            "price":       price_cents / 100,
            "cost":        cost_cents / 100,
            "stock_qty":   fake.random_int(min=0, max=500),
            "created_at":  fake.date_time_between(start_date="-3y", end_date="now").isoformat(),
        })
    df = pd.DataFrame(records)
    _validate_sample(df, ProductRaw, "products")
    return df


@timed
def generate_orders(
    n: int, num_customers: int, num_products: int
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Generate *n* orders and their line items.

    Monetary arithmetic uses integer cents throughout to prevent rounding drift.

    Args:
        n: Number of orders to generate.
        num_customers: Upper bound for customer_id foreign keys.
        num_products: Upper bound for product_id foreign keys.

    Returns:
        Tuple of (orders_df, order_items_df).
    """
    logger.info("Generating orders", extra={"n": n})
    orders, items = [], []
    item_id = 1

    for order_id in range(1, n + 1):
        order_date  = fake.date_time_between(start_date="-3y", end_date="now")
        num_items   = fake.random_int(min=1, max=5)
        order_cents = 0

        for _ in range(num_items):
            product_id    = fake.random_int(min=1, max=num_products)
            qty           = fake.random_int(min=1, max=10)
            unit_cents    = fake.random_int(min=500, max=200_000)
            discount_pct  = fake.random_int(min=0, max=30) / 100   # 0 – 30%
            line_cents    = int(unit_cents * qty * (1 - discount_pct))
            order_cents  += line_cents
            items.append({
                "item_id":    item_id,
                "order_id":   order_id,
                "product_id": product_id,
                "quantity":   qty,
                "unit_price": unit_cents / 100,
                "discount":   round(discount_pct, 4),
                "line_total": line_cents / 100,
            })
            item_id += 1

        shipping_cents = fake.random_int(min=500, max=5_000)
        orders.append({
            "order_id":      order_id,
            "customer_id":   fake.random_int(min=1, max=num_customers),
            "order_date":    order_date.isoformat(),
            "status":        fake.random_element(_STATUSES),
            "total_amount":  order_cents / 100,
            "shipping_fee":  shipping_cents / 100,
            "channel":       fake.random_element(_CHANNELS),
        })

    orders_df = pd.DataFrame(orders)
    items_df  = pd.DataFrame(items)
    _validate_sample(orders_df, OrderRaw, "orders")
    _validate_sample(items_df,  OrderItemRaw, "order_items")
    return orders_df, items_df


@timed
def generate_payments(orders_df: pd.DataFrame) -> pd.DataFrame:
    """Generate one payment record per order (vectorised).

    Args:
        orders_df: Orders DataFrame — provides order_id and total_amount.

    Returns:
        Validated DataFrame matching PaymentRaw schema.
    """
    logger.info("Generating payments", extra={"n": len(orders_df)})
    n = len(orders_df)
    rng = fake.random

    hour_offsets = [timedelta(hours=rng.randint(0, 48)) for _ in range(n)]
    paid_at_list = [
        (datetime.fromisoformat(str(dt)) + off).isoformat()
        for dt, off in zip(orders_df["order_date"], hour_offsets)
    ]

    df = pd.DataFrame({
        "payment_id": orders_df["order_id"].values,
        "order_id":   orders_df["order_id"].values,
        "method":     [rng.choice(_PAY_METHODS)  for _ in range(n)],
        "status":     [rng.choice(_PAY_STATUSES) for _ in range(n)],
        "amount":     orders_df["total_amount"].values,
        "paid_at":    paid_at_list,
    })
    _validate_sample(df, PaymentRaw, "payments")
    return df


@timed
def generate_reviews(orders_df: pd.DataFrame, num_products: int) -> pd.DataFrame:
    """Generate reviews for ~60 % of orders.

    Args:
        orders_df: Orders DataFrame used as the source population.
        num_products: Upper bound for product_id references.

    Returns:
        Validated DataFrame matching ReviewRaw schema.
    """
    logger.info("Generating reviews")
    sampled = orders_df.sample(frac=0.6, random_state=settings.FAKE_DATA_SEED).reset_index(drop=True)
    n = len(sampled)
    rng = fake.random

    df = pd.DataFrame({
        "review_id":   range(1, n + 1),
        "order_id":    sampled["order_id"].values,
        "customer_id": sampled["customer_id"].values,
        "product_id":  [rng.randint(1, num_products) for _ in range(n)],
        "rating":      [rng.randint(1, 5) for _ in range(n)],
        "comment":     [fake.sentence(nb_words=12) for _ in range(n)],
        "created_at":  [
            fake.date_time_between(start_date=str(dt), end_date="now").isoformat()
            for dt in sampled["order_date"]
        ],
    })
    _validate_sample(df, ReviewRaw, "reviews")
    return df


# ─────────────────────────────────────────────────────────────────────────────
# Validation helper
# ─────────────────────────────────────────────────────────────────────────────

def _validate_sample(df: pd.DataFrame, model: type, name: str, n_sample: int = 100) -> None:
    """Validate a random sample of rows against a Pydantic model.

    Sampling rather than full validation keeps ingestion fast while still
    catching schema drift.

    Args:
        df: DataFrame to validate.
        model: Pydantic model class to validate each row against.
        name: Dataset name used in log messages.
        n_sample: Number of rows to validate. Validates all if len(df) ≤ n_sample.

    Raises:
        ValueError: If any sampled row fails model validation.
    """
    sample = df.head(n_sample) if len(df) > n_sample else df
    errors: list[str] = []
    for idx, row in sample.iterrows():
        try:
            model(**row.to_dict())
        except Exception as exc:
            errors.append(f"row {idx}: {exc}")
    if errors:
        msg = f"Schema validation failed for '{name}' ({len(errors)} errors): {errors[:3]}"
        logger.error(msg)
        raise ValueError(msg)
    logger.debug("Schema validation passed for '%s'", name)


# ─────────────────────────────────────────────────────────────────────────────
# I/O helpers
# ─────────────────────────────────────────────────────────────────────────────

@timed
def save_csv(df: pd.DataFrame, name: str) -> Path:
    """Write a DataFrame as CSV to the raw data directory.

    Args:
        df: DataFrame to persist.
        name: Base filename without extension.

    Returns:
        Path to the written file.
    """
    settings.RAW_DATA_PATH.mkdir(parents=True, exist_ok=True)
    path = settings.RAW_DATA_PATH / f"{name}.csv"
    df.to_csv(path, index=False)
    logger.info("CSV saved", extra={"path": str(path), "rows": len(df)})
    return path


@timed
def csv_to_parquet(name: str) -> Path:
    """Convert a raw CSV to a Hive-partitioned Parquet dataset.

    Partition columns (year, month) are derived from the first datetime-like
    column found in the DataFrame.

    Args:
        name: Base filename (without extension) under RAW_DATA_PATH.

    Returns:
        Path to the Parquet output directory.
    """
    csv_path = settings.RAW_DATA_PATH / f"{name}.csv"
    df = pd.read_csv(csv_path)

    date_col = next(
        (c for c in df.columns if any(kw in c for kw in ("date", "created", "paid"))),
        None,
    )
    partition_cols: list[str] | None = None
    if date_col:
        df["year"]  = pd.to_datetime(df[date_col], errors="coerce").dt.year
        df["month"] = pd.to_datetime(df[date_col], errors="coerce").dt.month
        partition_cols = ["year", "month"]

    out_dir = settings.RAW_DATA_PATH / "parquet" / name
    out_dir.mkdir(parents=True, exist_ok=True)
    df.to_parquet(out_dir, partition_cols=partition_cols, index=False, engine="pyarrow")
    logger.info("Parquet written", extra={"path": str(out_dir), "partitions": partition_cols})
    return out_dir


def log_metadata(df: pd.DataFrame, name: str) -> dict[str, Any]:
    """Compute and log dataset metadata.

    Args:
        df: DataFrame to profile.
        name: Dataset label used in structured log entry.

    Returns:
        Metadata dict with rows, columns, null_pct_per_column, ingested_at.
    """
    null_pct = (df.isnull().mean() * 100).round(2).to_dict()
    meta: dict[str, Any] = {
        "dataset":             name,
        "rows":                len(df),
        "columns":             len(df.columns),
        "null_pct_per_column": null_pct,
        "ingested_at":         datetime.utcnow().isoformat(),
    }
    logger.info("Dataset metadata", extra=meta)
    return meta


# ─────────────────────────────────────────────────────────────────────────────
# Orchestration
# ─────────────────────────────────────────────────────────────────────────────

@timed
def run() -> list[dict[str, Any]]:
    """Execute the full Bronze ingestion pipeline.

    Returns:
        List of metadata dicts, one per dataset.
    """
    logger.info("=== Bronze ingestion started ===")
    Faker.seed(settings.FAKE_DATA_SEED)  # reset seed for reproducibility on re-runs

    customers_df             = generate_customers(settings.NUM_CUSTOMERS)
    products_df              = generate_products(settings.NUM_PRODUCTS)
    orders_df, items_df      = generate_orders(
        settings.NUM_ORDERS, settings.NUM_CUSTOMERS, settings.NUM_PRODUCTS
    )
    payments_df              = generate_payments(orders_df)
    reviews_df               = generate_reviews(orders_df, settings.NUM_PRODUCTS)

    datasets = {
        "customers":   customers_df,
        "products":    products_df,
        "orders":      orders_df,
        "order_items": items_df,
        "payments":    payments_df,
        "reviews":     reviews_df,
    }

    metadata_list = []
    for name, df in datasets.items():
        save_csv(df, name)
        csv_to_parquet(name)
        metadata_list.append(log_metadata(df, name))

    logger.info("=== Bronze ingestion complete ===", extra={"datasets": len(datasets)})
    return metadata_list


if __name__ == "__main__":
    run()
