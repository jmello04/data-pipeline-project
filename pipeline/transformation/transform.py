"""
pipeline/transformation/transform.py

Silver layer: data cleaning, LGPD anonymisation, and PySpark transformations.

Key design decisions:
    - HMAC-SHA256 (not plain SHA-256) for LGPD anonymisation.
      Plain SHA-256 is deterministic and searchable via rainbow tables.
      HMAC requires the secret key to reproduce — without it the hash is
      computationally irreversible.
    - A single _anonymise() function is used everywhere so all three tables
      (customers, orders, reviews) hash customer_id identically.  This
      preserves referential integrity after anonymisation.
    - Spark DataFrames are created with explicit StructType schemas — no
      schema inference.  Inference is slow and produces wrong types for
      nullable columns.
    - Dimension tables are broadcast to the Spark executors to avoid
      expensive shuffle joins.
    - All coercions that produce NaN are counted and logged.
"""

import hashlib
import hmac
import logging
from pathlib import Path

import pandas as pd
from pyspark.sql import SparkSession, DataFrame as SparkDF
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType, IntegerType, LongType, StringType, StructField, StructType, TimestampType,
)
from pyspark.sql.window import Window

from config.settings import settings
from pipeline.utils.decorators import timed
from pipeline.utils.logging_config import get_logger

logger = get_logger(__name__, log_file=settings.LOGS_PATH / "transformation.log")


# ─────────────────────────────────────────────────────────────────────────────
# LGPD anonymisation
# ─────────────────────────────────────────────────────────────────────────────

def _anonymise(value: object) -> str:
    """Return the HMAC-SHA256 hex digest of *value*.

    Uses the LGPD_HASH_KEY secret so the same value always produces the same
    hash across all tables — preserving FK relationships — while preventing
    reconstruction without the key.

    Why HMAC over plain SHA-256:
        SHA-256("alice@example.com") is static and can be looked up in
        pre-computed tables.  HMAC-SHA256("alice@example.com", key=secret)
        cannot be pre-computed without the secret.

    Args:
        value: PII value to anonymise.  None is hashed as the empty string
            and produces a consistent sentinel value.

    Returns:
        64-character lowercase hex string.
    """
    raw = "" if value is None else str(value)
    return hmac.new(
        settings.LGPD_HASH_KEY.encode(),
        raw.encode(),
        hashlib.sha256,
    ).hexdigest()


def _anonymise_series(series: pd.Series) -> pd.Series:
    """Vectorised anonymisation of a pandas Series.

    Args:
        series: Series of PII values.

    Returns:
        Series of 64-character hex strings.
    """
    return series.map(_anonymise)


# ─────────────────────────────────────────────────────────────────────────────
# Spark helpers
# ─────────────────────────────────────────────────────────────────────────────

def _get_spark() -> SparkSession:
    """Build or retrieve the shared SparkSession.

    Returns:
        Active SparkSession.
    """
    return (
        SparkSession.builder
        .appName(settings.SPARK_APP_NAME)
        .master(settings.SPARK_MASTER)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.adaptive.enabled", "true")          # adaptive query execution
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )


# ─────────────────────────────────────────────────────────────────────────────
# Spark schemas (explicit — no inference)
# ─────────────────────────────────────────────────────────────────────────────

_ORDERS_SCHEMA = StructType([
    StructField("order_id",     LongType(),   nullable=False),
    StructField("customer_id",  StringType(), nullable=False),  # already HMAC-hashed by pandas pass
    StructField("order_date",   TimestampType(), nullable=True),
    StructField("status",       StringType(), nullable=True),
    StructField("total_amount", DoubleType(), nullable=True),
    StructField("shipping_fee", DoubleType(), nullable=True),
    StructField("channel",      StringType(), nullable=True),
])

_ITEMS_SCHEMA = StructType([
    StructField("item_id",    LongType(),   nullable=False),
    StructField("order_id",   LongType(),   nullable=False),
    StructField("product_id", LongType(),   nullable=False),
    StructField("quantity",   IntegerType(), nullable=True),
    StructField("unit_price", DoubleType(), nullable=True),
    StructField("discount",   DoubleType(), nullable=True),
    StructField("line_total", DoubleType(), nullable=True),
])

_PRODUCTS_SCHEMA = StructType([
    StructField("product_id",  LongType(),   nullable=False),
    StructField("name",        StringType(), nullable=True),
    StructField("category",    StringType(), nullable=True),
    StructField("subcategory", StringType(), nullable=True),
    StructField("price",       DoubleType(), nullable=True),
    StructField("cost",        DoubleType(), nullable=True),
    StructField("stock_qty",   IntegerType(), nullable=True),
])

_PAYMENTS_SCHEMA = StructType([
    StructField("payment_id", LongType(),   nullable=False),
    StructField("order_id",   LongType(),   nullable=False),
    StructField("method",     StringType(), nullable=True),
    StructField("status",     StringType(), nullable=True),
    StructField("amount",     DoubleType(), nullable=True),
])

_REVIEWS_SCHEMA = StructType([
    StructField("review_id",  LongType(),    nullable=False),
    StructField("order_id",   LongType(),    nullable=False),
    StructField("customer_id",StringType(), nullable=False),  # HMAC-hashed
    StructField("product_id", LongType(),   nullable=False),
    StructField("rating",     IntegerType(), nullable=True),
])


# ─────────────────────────────────────────────────────────────────────────────
# Pandas cleaning pass
# ─────────────────────────────────────────────────────────────────────────────

def _count_coercions(before: pd.Series, after: pd.Series, col: str, dataset: str) -> None:
    """Log how many values were coerced to NaN.

    Args:
        before: Series before coercion.
        after: Series after coercion.
        col: Column name for the log message.
        dataset: Dataset name for the log message.
    """
    n_lost = int(after.isna().sum() - before.isna().sum())
    if n_lost > 0:
        logger.warning(
            "Coercion produced NaN", extra={"dataset": dataset, "column": col, "n_lost": n_lost}
        )


@timed
def clean_customers(df: pd.DataFrame) -> pd.DataFrame:
    """Clean and anonymise the customers dataset.

    Anonymisation is applied AFTER deduplication so the hash corpus is
    canonical — every downstream table that hashes the same original
    customer_id will produce the same digest.

    Args:
        df: Raw customers DataFrame.

    Returns:
        Cleaned, LGPD-compliant DataFrame.
    """
    logger.info("Cleaning customers", extra={"rows_in": len(df)})
    df = df.drop_duplicates().dropna(subset=["customer_id", "email"])
    df["created_at"] = pd.to_datetime(df["created_at"], errors="coerce")
    df["birth_date"]  = pd.to_datetime(df["birth_date"],  errors="coerce")

    # LGPD: anonymise PII — applied last to ensure hash key matches FK tables
    for col in ("customer_id", "name", "email"):
        df[col] = _anonymise_series(df[col])

    logger.info("Customers cleaned", extra={"rows_out": len(df)})
    return df.reset_index(drop=True)


@timed
def clean_products(df: pd.DataFrame) -> pd.DataFrame:
    """Clean the products dataset.

    Args:
        df: Raw products DataFrame.

    Returns:
        Cleaned DataFrame.
    """
    logger.info("Cleaning products", extra={"rows_in": len(df)})
    df = df.drop_duplicates(subset=["product_id"])
    df["stock_qty"] = df["stock_qty"].fillna(0).astype(int)

    for col in ("price", "cost"):
        before = df[col].copy()
        df[col] = pd.to_numeric(df[col], errors="coerce").abs()
        _count_coercions(before, df[col], col, "products")

    # Guard: cost must not exceed price to prevent negative margin
    mask = df["cost"] > df["price"]
    if mask.any():
        logger.warning("Capping cost to price for %d rows", mask.sum())
        df.loc[mask, "cost"] = df.loc[mask, "price"]

    df["created_at"] = pd.to_datetime(df["created_at"], errors="coerce")
    logger.info("Products cleaned", extra={"rows_out": len(df)})
    return df.reset_index(drop=True)


@timed
def clean_orders(df: pd.DataFrame) -> pd.DataFrame:
    """Clean the orders dataset and anonymise customer_id.

    The customer_id is hashed with the same HMAC key used in clean_customers,
    so FK joins between orders and customers remain valid after anonymisation.

    Args:
        df: Raw orders DataFrame.

    Returns:
        Cleaned, LGPD-compliant DataFrame.
    """
    logger.info("Cleaning orders", extra={"rows_in": len(df)})
    df = df.drop_duplicates(subset=["order_id"])
    df["order_date"] = pd.to_datetime(df["order_date"], errors="coerce")
    df = df[df["total_amount"].notna() & (df["total_amount"] > 0)]

    # Anonymise FK so it matches hashed customers table
    df["customer_id"] = _anonymise_series(df["customer_id"])

    logger.info("Orders cleaned", extra={"rows_out": len(df)})
    return df.reset_index(drop=True)


@timed
def clean_order_items(df: pd.DataFrame) -> pd.DataFrame:
    """Clean the order_items dataset.

    Args:
        df: Raw order_items DataFrame.

    Returns:
        Cleaned DataFrame.
    """
    logger.info("Cleaning order_items", extra={"rows_in": len(df)})
    df = df.drop_duplicates(subset=["item_id"])
    df["unit_price"] = pd.to_numeric(df["unit_price"], errors="coerce").abs()
    df["discount"]   = pd.to_numeric(df["discount"],   errors="coerce").clip(0.0, 1.0)
    df["line_total"] = pd.to_numeric(df["line_total"],  errors="coerce").abs()
    logger.info("Order items cleaned", extra={"rows_out": len(df)})
    return df.reset_index(drop=True)


@timed
def clean_payments(df: pd.DataFrame) -> pd.DataFrame:
    """Clean the payments dataset.

    Args:
        df: Raw payments DataFrame.

    Returns:
        Cleaned DataFrame.
    """
    logger.info("Cleaning payments", extra={"rows_in": len(df)})
    df = df.drop_duplicates(subset=["payment_id"])
    df["paid_at"] = pd.to_datetime(df["paid_at"], errors="coerce")
    logger.info("Payments cleaned", extra={"rows_out": len(df)})
    return df.reset_index(drop=True)


@timed
def clean_reviews(df: pd.DataFrame) -> pd.DataFrame:
    """Clean and anonymise the reviews dataset.

    Args:
        df: Raw reviews DataFrame.

    Returns:
        Cleaned, LGPD-compliant DataFrame.
    """
    logger.info("Cleaning reviews", extra={"rows_in": len(df)})
    df = df.drop_duplicates(subset=["review_id"])
    df["rating"] = pd.to_numeric(df["rating"], errors="coerce").clip(1, 5).astype("Int64")

    # Anonymise FK — same HMAC key → same hash → FK joins work
    df["customer_id"] = _anonymise_series(df["customer_id"])

    logger.info("Reviews cleaned", extra={"rows_out": len(df)})
    return df.reset_index(drop=True)


# ─────────────────────────────────────────────────────────────────────────────
# PySpark transformation pass
# ─────────────────────────────────────────────────────────────────────────────

@timed
def spark_transform(spark: SparkSession) -> dict[str, SparkDF]:
    """Apply PySpark enrichment: joins, aggregations, window functions.

    Dimension tables (products, payments slim) are broadcast explicitly to
    avoid shuffle joins.  Adaptive Query Execution handles the rest.

    Args:
        spark: Active SparkSession.

    Returns:
        Dict mapping output names to Spark DataFrames.
    """
    cleaned = settings.PROCESSED_DATA_PATH / "cleaned"
    logger.info("Starting Spark transformations", extra={"source": str(cleaned)})

    # ── Load with explicit schemas ─────────────────────────────────────────
    customers = spark.read.parquet(str(cleaned / "customers"))
    orders    = spark.read.schema(_ORDERS_SCHEMA).parquet(str(cleaned / "orders"))
    items     = spark.read.schema(_ITEMS_SCHEMA).parquet(str(cleaned / "order_items"))
    products  = spark.read.schema(_PRODUCTS_SCHEMA).parquet(str(cleaned / "products"))
    payments  = spark.read.schema(_PAYMENTS_SCHEMA).parquet(str(cleaned / "payments"))
    reviews   = spark.read.schema(_REVIEWS_SCHEMA).parquet(str(cleaned / "reviews"))

    # ── Broadcast small dimension tables ──────────────────────────────────
    products_bc  = F.broadcast(products.select("product_id", "category", "subcategory"))
    payments_bc  = F.broadcast(
        payments.select("order_id", "method", F.col("status").alias("payment_status"))
    )

    # ── Enriched orders ────────────────────────────────────────────────────
    orders_enriched = (
        orders
        .join(customers.select("customer_id", "city", "state"), on="customer_id", how="left")
        .join(payments_bc, on="order_id", how="left")
        .withColumn("year",  F.year("order_date"))
        .withColumn("month", F.month("order_date"))
    )

    items_enriched = items.join(products_bc, on="product_id", how="left")

    # ── Monthly revenue per category (delivered orders) ────────────────────
    monthly_revenue = (
        orders_enriched
        .join(items_enriched, on="order_id", how="left")
        .filter(F.col("status") == "delivered")
        .groupBy("year", "month", "category")
        .agg(
            F.sum("line_total").alias("revenue"),
            F.countDistinct("order_id").alias("order_count"),
            F.countDistinct("customer_id").alias("unique_customers"),
        )
        .orderBy("year", "month", F.desc("revenue"))
    )

    # ── Customer spend with window functions ───────────────────────────────
    # Tie-break by order_id to make ranking fully deterministic.
    cust_window = Window.partitionBy("customer_id").orderBy("order_date", "order_id")

    customer_spend = (
        orders.select("customer_id", "order_id", "order_date", "total_amount")
        .withColumn("running_total",       F.sum("total_amount").over(cust_window))
        .withColumn("order_rank",          F.rank().over(cust_window))
        .withColumn("prev_order_amount",   F.lag("total_amount", 1).over(cust_window))
    )

    # ── Product rating summary ─────────────────────────────────────────────
    product_ratings = (
        reviews
        .groupBy("product_id")
        .agg(
            F.avg("rating").alias("avg_rating"),
            F.count("review_id").alias("review_count"),
        )
    )

    logger.info("Spark transformations complete")
    return {
        "orders_enriched": orders_enriched,
        "items_enriched":  items_enriched,
        "monthly_revenue": monthly_revenue,
        "customer_spend":  customer_spend,
        "product_ratings": product_ratings,
    }


# ─────────────────────────────────────────────────────────────────────────────
# Persistence
# ─────────────────────────────────────────────────────────────────────────────

def _save_pandas(df: pd.DataFrame, name: str) -> None:
    """Write a pandas DataFrame to the Silver cleaned layer.

    Args:
        df: DataFrame to persist.
        name: Sub-directory name under PROCESSED_DATA_PATH/cleaned/.
    """
    out = settings.PROCESSED_DATA_PATH / "cleaned" / name
    out.mkdir(parents=True, exist_ok=True)
    df.to_parquet(out, index=False, engine="pyarrow")
    logger.info("Saved cleaned dataset", extra={"path": str(out), "rows": len(df)})


def _save_spark(sdf: SparkDF, name: str, partition_cols: list[str] | None = None) -> None:
    """Write a Spark DataFrame to the Silver spark layer.

    Args:
        sdf: Spark DataFrame to persist.
        name: Sub-directory name.
        partition_cols: Optional Hive-style partition columns.
    """
    out = str(settings.PROCESSED_DATA_PATH / "spark" / name)
    writer = sdf.write.mode("overwrite").format("parquet")
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)
    writer.save(out)
    logger.info("Saved Spark output", extra={"path": out, "partitions": partition_cols})


# ─────────────────────────────────────────────────────────────────────────────
# Orchestration
# ─────────────────────────────────────────────────────────────────────────────

@timed
def run() -> None:
    """Execute the full Silver transformation pipeline."""
    logger.info("=== Silver transformation started ===")
    raw = settings.RAW_DATA_PATH

    cleaners: list[tuple[str, object, pd.DataFrame]] = [
        ("customers",   clean_customers,   pd.read_csv(raw / "customers.csv")),
        ("products",    clean_products,    pd.read_csv(raw / "products.csv")),
        ("orders",      clean_orders,      pd.read_csv(raw / "orders.csv")),
        ("order_items", clean_order_items, pd.read_csv(raw / "order_items.csv")),
        ("payments",    clean_payments,    pd.read_csv(raw / "payments.csv")),
        ("reviews",     clean_reviews,     pd.read_csv(raw / "reviews.csv")),
    ]
    for name, fn, df in cleaners:
        _save_pandas(fn(df), name)

    spark = _get_spark()
    try:
        outputs = spark_transform(spark)
        partition_map: dict[str, list[str] | None] = {
            "orders_enriched": ["year", "month"],
            "monthly_revenue": ["year", "month"],
            "customer_spend":  None,
            "items_enriched":  None,
            "product_ratings": None,
        }
        for name, sdf in outputs.items():
            _save_spark(sdf, name, partition_map.get(name))
    finally:
        spark.stop()

    logger.info("=== Silver transformation complete ===")


if __name__ == "__main__":
    run()
