"""
pipeline/ingestion/ingest.py

Bronze layer: synthetic data generation and raw-to-Parquet ingestion.

Responsibilities:
    1. Generate synthetic e-commerce data (customers, products, orders,
       payments, reviews) using Faker and persist as CSV in data/raw/.
    2. Read each CSV and write partitioned Parquet files to data/raw/parquet/.
    3. Log metadata (row counts, column counts, null percentages) to
       logs/ingestion.log.
"""

import hashlib
import logging
import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import pandas as pd
from faker import Faker

from config.settings import settings

# ── Logging setup ──────────────────────────────────────────────────────────────
settings.LOGS_PATH.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s – %(message)s",
    handlers=[
        logging.FileHandler(settings.LOGS_PATH / "ingestion.log"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)

# ── Faker initialisation ───────────────────────────────────────────────────────
fake = Faker("pt_BR")
Faker.seed(settings.FAKE_DATA_SEED)


# ─────────────────────────────────────────────────────────────────────────────
# Generators
# ─────────────────────────────────────────────────────────────────────────────


def generate_customers(n: int) -> pd.DataFrame:
    """Generate synthetic customer records.

    Args:
        n: Number of customer records to generate.

    Returns:
        DataFrame with columns: customer_id, name, email, phone, city,
        state, birth_date, created_at.
    """
    logger.info("Generating %d customer records …", n)
    records = []
    for i in range(1, n + 1):
        records.append(
            {
                "customer_id": i,
                "name": fake.name(),
                "email": fake.email(),
                "phone": fake.phone_number(),
                "city": fake.city(),
                "state": fake.state_abbr(),
                "birth_date": fake.date_of_birth(minimum_age=18, maximum_age=80).isoformat(),
                "created_at": fake.date_time_between(
                    start_date="-5y", end_date="now"
                ).isoformat(),
            }
        )
    return pd.DataFrame(records)


def generate_products(n: int) -> pd.DataFrame:
    """Generate synthetic product records.

    Args:
        n: Number of product records to generate.

    Returns:
        DataFrame with columns: product_id, name, category, subcategory,
        price, cost, stock_qty, created_at.
    """
    logger.info("Generating %d product records …", n)
    categories = {
        "Electronics": ["Smartphones", "Laptops", "Accessories"],
        "Clothing": ["Men", "Women", "Kids"],
        "Home": ["Furniture", "Kitchen", "Decor"],
        "Sports": ["Fitness", "Outdoor", "Team Sports"],
        "Books": ["Fiction", "Non-Fiction", "Technical"],
    }
    records = []
    for i in range(1, n + 1):
        category = fake.random_element(list(categories.keys()))
        subcategory = fake.random_element(categories[category])
        price = round(fake.pyfloat(min_value=5, max_value=2000, right_digits=2), 2)
        records.append(
            {
                "product_id": i,
                "name": fake.catch_phrase(),
                "category": category,
                "subcategory": subcategory,
                "price": price,
                "cost": round(price * fake.pyfloat(min_value=0.3, max_value=0.8, right_digits=2), 2),
                "stock_qty": fake.random_int(min=0, max=500),
                "created_at": fake.date_time_between(start_date="-3y", end_date="now").isoformat(),
            }
        )
    return pd.DataFrame(records)


def generate_orders(n: int, num_customers: int, num_products: int) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Generate synthetic order and order-item records.

    Args:
        n: Number of orders to generate.
        num_customers: Upper bound for customer_id references.
        num_products: Upper bound for product_id references.

    Returns:
        Tuple of (orders_df, order_items_df).
    """
    logger.info("Generating %d order records …", n)
    statuses = ["pending", "processing", "shipped", "delivered", "cancelled", "returned"]
    orders, items = [], []
    item_id = 1
    for order_id in range(1, n + 1):
        order_date = fake.date_time_between(start_date="-3y", end_date="now")
        status = fake.random_element(statuses)
        num_items = fake.random_int(min=1, max=5)
        order_total = 0.0

        for _ in range(num_items):
            product_id = fake.random_int(min=1, max=num_products)
            qty = fake.random_int(min=1, max=10)
            unit_price = round(fake.pyfloat(min_value=5, max_value=2000, right_digits=2), 2)
            discount = round(fake.pyfloat(min_value=0, max_value=0.3, right_digits=2), 2)
            line_total = round(unit_price * qty * (1 - discount), 2)
            order_total += line_total
            items.append(
                {
                    "item_id": item_id,
                    "order_id": order_id,
                    "product_id": product_id,
                    "quantity": qty,
                    "unit_price": unit_price,
                    "discount": discount,
                    "line_total": line_total,
                }
            )
            item_id += 1

        orders.append(
            {
                "order_id": order_id,
                "customer_id": fake.random_int(min=1, max=num_customers),
                "order_date": order_date.isoformat(),
                "status": status,
                "total_amount": round(order_total, 2),
                "shipping_fee": round(fake.pyfloat(min_value=5, max_value=50, right_digits=2), 2),
                "channel": fake.random_element(["web", "mobile", "marketplace"]),
            }
        )
    return pd.DataFrame(orders), pd.DataFrame(items)


def generate_payments(orders_df: pd.DataFrame) -> pd.DataFrame:
    """Generate one payment record per order.

    Args:
        orders_df: Orders DataFrame used to derive payment references.

    Returns:
        DataFrame with columns: payment_id, order_id, method, status,
        amount, paid_at.
    """
    logger.info("Generating payment records …")
    methods = ["credit_card", "debit_card", "pix", "boleto", "wallet"]
    statuses = ["approved", "declined", "pending", "refunded"]
    records = []
    for _, row in orders_df.iterrows():
        paid_at = (
            datetime.fromisoformat(str(row["order_date"])) + timedelta(hours=fake.random_int(0, 48))
        ).isoformat()
        records.append(
            {
                "payment_id": row["order_id"],
                "order_id": row["order_id"],
                "method": fake.random_element(methods),
                "status": fake.random_element(statuses),
                "amount": row["total_amount"],
                "paid_at": paid_at,
            }
        )
    return pd.DataFrame(records)


def generate_reviews(orders_df: pd.DataFrame, num_products: int) -> pd.DataFrame:
    """Generate product review records (not every order has a review).

    Args:
        orders_df: Orders DataFrame; ~60 % of orders will produce a review.
        num_products: Upper bound for product_id references.

    Returns:
        DataFrame with columns: review_id, order_id, customer_id,
        product_id, rating, comment, created_at.
    """
    logger.info("Generating review records …")
    reviewed = orders_df.sample(frac=0.6, random_state=settings.FAKE_DATA_SEED)
    records = []
    for review_id, (_, row) in enumerate(reviewed.iterrows(), start=1):
        records.append(
            {
                "review_id": review_id,
                "order_id": int(row["order_id"]),
                "customer_id": int(row["customer_id"]),
                "product_id": fake.random_int(min=1, max=num_products),
                "rating": fake.random_int(min=1, max=5),
                "comment": fake.sentence(nb_words=12),
                "created_at": fake.date_time_between(
                    start_date=row["order_date"], end_date="now"
                ).isoformat(),
            }
        )
    return pd.DataFrame(records)


# ─────────────────────────────────────────────────────────────────────────────
# I/O helpers
# ─────────────────────────────────────────────────────────────────────────────


def save_csv(df: pd.DataFrame, name: str) -> Path:
    """Persist a DataFrame as CSV in the raw data directory.

    Args:
        df: DataFrame to save.
        name: Base file name (without extension).

    Returns:
        Path to the written file.
    """
    settings.RAW_DATA_PATH.mkdir(parents=True, exist_ok=True)
    path = settings.RAW_DATA_PATH / f"{name}.csv"
    df.to_csv(path, index=False)
    logger.info("CSV saved → %s (%d rows)", path, len(df))
    return path


def csv_to_parquet(name: str, partition_cols: list[str] | None = None) -> Path:
    """Read a raw CSV and write a partitioned Parquet dataset.

    Args:
        name: Base file name (without extension) under RAW_DATA_PATH.
        partition_cols: Column names used for Hive-style partitioning.

    Returns:
        Path to the Parquet output directory.
    """
    csv_path = settings.RAW_DATA_PATH / f"{name}.csv"
    df = pd.read_csv(csv_path)

    # Derive year/month partition columns from the first datetime-ish column
    if partition_cols is None:
        date_cols = [c for c in df.columns if "date" in c or "created" in c or "paid" in c]
        if date_cols:
            df["year"] = pd.to_datetime(df[date_cols[0]]).dt.year
            df["month"] = pd.to_datetime(df[date_cols[0]]).dt.month
            partition_cols = ["year", "month"]

    out_dir = settings.RAW_DATA_PATH / "parquet" / name
    out_dir.mkdir(parents=True, exist_ok=True)

    df.to_parquet(out_dir, partition_cols=partition_cols, index=False, engine="pyarrow")
    logger.info("Parquet written → %s (partitions: %s)", out_dir, partition_cols)
    return out_dir


def log_metadata(df: pd.DataFrame, name: str) -> dict[str, Any]:
    """Compute and log DataFrame metadata.

    Args:
        df: DataFrame to profile.
        name: Human-readable dataset label used in log messages.

    Returns:
        Dictionary with rows, columns, null_pct_per_column.
    """
    null_pct = (df.isnull().sum() / len(df) * 100).round(2).to_dict()
    meta: dict[str, Any] = {
        "dataset": name,
        "rows": len(df),
        "columns": len(df.columns),
        "null_pct_per_column": null_pct,
        "ingested_at": datetime.utcnow().isoformat(),
    }
    logger.info(
        "Metadata [%s]: rows=%d cols=%d nulls=%s",
        name, meta["rows"], meta["columns"], null_pct,
    )
    return meta


# ─────────────────────────────────────────────────────────────────────────────
# Orchestration
# ─────────────────────────────────────────────────────────────────────────────


def run() -> list[dict[str, Any]]:
    """Execute the full Bronze ingestion pipeline.

    Generates all synthetic datasets, saves CSVs, converts to partitioned
    Parquet, and returns a list of metadata dictionaries.

    Returns:
        List of metadata dicts, one per dataset.
    """
    logger.info("=== Bronze ingestion started ===")

    customers_df = generate_customers(settings.NUM_CUSTOMERS)
    products_df = generate_products(settings.NUM_PRODUCTS)
    orders_df, items_df = generate_orders(
        settings.NUM_ORDERS, settings.NUM_CUSTOMERS, settings.NUM_PRODUCTS
    )
    payments_df = generate_payments(orders_df)
    reviews_df = generate_reviews(orders_df, settings.NUM_PRODUCTS)

    datasets = {
        "customers": customers_df,
        "products": products_df,
        "orders": orders_df,
        "order_items": items_df,
        "payments": payments_df,
        "reviews": reviews_df,
    }

    metadata_list = []
    for name, df in datasets.items():
        save_csv(df, name)
        csv_to_parquet(name)
        metadata_list.append(log_metadata(df, name))

    logger.info("=== Bronze ingestion complete ===")
    return metadata_list


if __name__ == "__main__":
    run()
