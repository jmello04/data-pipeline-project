"""
pipeline/transformation/transform.py

Silver layer: data cleaning, LGPD anonymisation, and PySpark transformations.

Responsibilities:
    1. Pandas pass – remove duplicates, coerce types, fill/drop nulls.
    2. LGPD pass – hash customer_id, name, email (SHA-256).
    3. PySpark pass – cross-table joins, aggregations, window functions.
    4. Persist cleaned datasets to data/processed/ (partitioned Parquet).
"""

import hashlib
import logging
from pathlib import Path

import pandas as pd
from pyspark.sql import SparkSession, DataFrame as SparkDF
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from config.settings import settings

# ── Logging ────────────────────────────────────────────────────────────────────
settings.LOGS_PATH.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s – %(message)s",
    handlers=[
        logging.FileHandler(settings.LOGS_PATH / "transformation.log"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────


def _sha256(value: str) -> str:
    """Return the hex SHA-256 digest of *value*.

    Args:
        value: Plain-text string to hash.

    Returns:
        64-character lowercase hexadecimal string.
    """
    return hashlib.sha256(str(value).encode()).hexdigest()


def _get_spark() -> SparkSession:
    """Build or retrieve the shared SparkSession.

    Returns:
        Active SparkSession configured from settings.
    """
    return (
        SparkSession.builder.appName(settings.SPARK_APP_NAME)
        .master(settings.SPARK_MASTER)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )


# ─────────────────────────────────────────────────────────────────────────────
# Pandas cleaning pass
# ─────────────────────────────────────────────────────────────────────────────


def clean_customers(df: pd.DataFrame) -> pd.DataFrame:
    """Clean and anonymise the customers dataset.

    Operations:
        - Drop exact duplicates.
        - Drop rows with null customer_id or email.
        - Coerce created_at / birth_date to datetime.
        - Apply LGPD anonymisation (SHA-256) to customer_id, name, email.

    Args:
        df: Raw customers DataFrame.

    Returns:
        Cleaned and anonymised DataFrame.
    """
    logger.info("Cleaning customers …")
    df = df.drop_duplicates()
    df = df.dropna(subset=["customer_id", "email"])
    df["created_at"] = pd.to_datetime(df["created_at"], errors="coerce")
    df["birth_date"] = pd.to_datetime(df["birth_date"], errors="coerce")

    # LGPD – hash PII
    df["customer_id"] = df["customer_id"].apply(_sha256)
    df["name"] = df["name"].apply(_sha256)
    df["email"] = df["email"].apply(_sha256)

    logger.info("Customers cleaned: %d rows", len(df))
    return df


def clean_products(df: pd.DataFrame) -> pd.DataFrame:
    """Clean the products dataset.

    Operations:
        - Drop duplicates on product_id.
        - Fill null stock_qty with 0.
        - Ensure price and cost are positive floats.

    Args:
        df: Raw products DataFrame.

    Returns:
        Cleaned DataFrame.
    """
    logger.info("Cleaning products …")
    df = df.drop_duplicates(subset=["product_id"])
    df["stock_qty"] = df["stock_qty"].fillna(0).astype(int)
    df["price"] = pd.to_numeric(df["price"], errors="coerce").abs()
    df["cost"] = pd.to_numeric(df["cost"], errors="coerce").abs()
    df["created_at"] = pd.to_datetime(df["created_at"], errors="coerce")
    logger.info("Products cleaned: %d rows", len(df))
    return df


def clean_orders(df: pd.DataFrame) -> pd.DataFrame:
    """Clean the orders dataset.

    Operations:
        - Drop duplicates on order_id.
        - Coerce order_date to datetime.
        - Drop rows where total_amount is null or negative.
        - Anonymise customer_id (SHA-256) to match LGPD-hashed customers.

    Args:
        df: Raw orders DataFrame.

    Returns:
        Cleaned DataFrame.
    """
    logger.info("Cleaning orders …")
    df = df.drop_duplicates(subset=["order_id"])
    df["order_date"] = pd.to_datetime(df["order_date"], errors="coerce")
    df = df[df["total_amount"].notna() & (df["total_amount"] > 0)]

    # Anonymise FK so it matches hashed customers table
    df["customer_id"] = df["customer_id"].apply(_sha256)

    logger.info("Orders cleaned: %d rows", len(df))
    return df


def clean_order_items(df: pd.DataFrame) -> pd.DataFrame:
    """Clean the order_items dataset.

    Args:
        df: Raw order_items DataFrame.

    Returns:
        Cleaned DataFrame.
    """
    logger.info("Cleaning order_items …")
    df = df.drop_duplicates(subset=["item_id"])
    df["unit_price"] = pd.to_numeric(df["unit_price"], errors="coerce").abs()
    df["discount"] = pd.to_numeric(df["discount"], errors="coerce").clip(0, 1)
    df["line_total"] = pd.to_numeric(df["line_total"], errors="coerce").abs()
    logger.info("Order items cleaned: %d rows", len(df))
    return df


def clean_payments(df: pd.DataFrame) -> pd.DataFrame:
    """Clean the payments dataset.

    Args:
        df: Raw payments DataFrame.

    Returns:
        Cleaned DataFrame.
    """
    logger.info("Cleaning payments …")
    df = df.drop_duplicates(subset=["payment_id"])
    df["paid_at"] = pd.to_datetime(df["paid_at"], errors="coerce")
    logger.info("Payments cleaned: %d rows", len(df))
    return df


def clean_reviews(df: pd.DataFrame) -> pd.DataFrame:
    """Clean the reviews dataset.

    Args:
        df: Raw reviews DataFrame.

    Returns:
        Cleaned DataFrame.
    """
    logger.info("Cleaning reviews …")
    df = df.drop_duplicates(subset=["review_id"])
    df["rating"] = pd.to_numeric(df["rating"], errors="coerce").clip(1, 5).astype("Int64")
    df["customer_id"] = df["customer_id"].apply(_sha256)
    logger.info("Reviews cleaned: %d rows", len(df))
    return df


# ─────────────────────────────────────────────────────────────────────────────
# PySpark transformation pass
# ─────────────────────────────────────────────────────────────────────────────


def spark_transform(spark: SparkSession) -> dict[str, SparkDF]:
    """Apply PySpark transformations: joins, aggregations, window functions.

    Reads cleaned Silver CSVs, enriches and aggregates data, then returns
    named DataFrames ready for downstream consumption.

    Args:
        spark: Active SparkSession.

    Returns:
        Dictionary mapping output names to Spark DataFrames.
    """
    processed = settings.PROCESSED_DATA_PATH / "cleaned"
    logger.info("Starting PySpark transformations …")

    # ── Load ────────────────────────────────────────────────────────────────
    customers = spark.read.parquet(str(processed / "customers"))
    orders = spark.read.parquet(str(processed / "orders"))
    items = spark.read.parquet(str(processed / "order_items"))
    products = spark.read.parquet(str(processed / "products"))
    payments = spark.read.parquet(str(processed / "payments"))
    reviews = spark.read.parquet(str(processed / "reviews"))

    # ── Join: orders + items + products ─────────────────────────────────────
    orders_enriched = (
        orders
        .join(customers, on="customer_id", how="left")
        .join(payments.select("order_id", "method", F.col("status").alias("payment_status")),
              on="order_id", how="left")
        .withColumn("year", F.year("order_date"))
        .withColumn("month", F.month("order_date"))
    )

    items_enriched = (
        items
        .join(products.select("product_id", "category", "subcategory"),
              on="product_id", how="left")
    )

    # ── Aggregation: monthly revenue per category ────────────────────────────
    orders_with_items = (
        orders_enriched
        .join(items_enriched, on="order_id", how="left")
    )

    monthly_revenue = (
        orders_with_items
        .filter(F.col("status") == "delivered")
        .groupBy("year", "month", "category")
        .agg(
            F.sum("line_total").alias("revenue"),
            F.count("order_id").alias("order_count"),
            F.countDistinct("customer_id").alias("unique_customers"),
        )
        .orderBy("year", "month", F.desc("revenue"))
    )

    # ── Window: customer running total of spend ──────────────────────────────
    customer_window = Window.partitionBy("customer_id").orderBy("order_date")

    customer_spend = (
        orders
        .select("customer_id", "order_id", "order_date", "total_amount")
        .withColumn("running_total", F.sum("total_amount").over(customer_window))
        .withColumn("order_rank", F.rank().over(customer_window))
        .withColumn("prev_order_amount", F.lag("total_amount", 1).over(customer_window))
    )

    # ── Average product rating ───────────────────────────────────────────────
    product_ratings = (
        reviews
        .groupBy("product_id")
        .agg(
            F.avg("rating").alias("avg_rating"),
            F.count("review_id").alias("review_count"),
        )
    )

    logger.info("PySpark transformations complete")
    return {
        "orders_enriched": orders_enriched,
        "items_enriched": items_enriched,
        "monthly_revenue": monthly_revenue,
        "customer_spend": customer_spend,
        "product_ratings": product_ratings,
    }


# ─────────────────────────────────────────────────────────────────────────────
# Persistence helpers
# ─────────────────────────────────────────────────────────────────────────────


def save_cleaned_pandas(df: pd.DataFrame, name: str) -> None:
    """Persist a cleaned pandas DataFrame as Parquet.

    Args:
        df: Cleaned DataFrame.
        name: Dataset name used as sub-directory.
    """
    out = settings.PROCESSED_DATA_PATH / "cleaned" / name
    out.mkdir(parents=True, exist_ok=True)
    df.to_parquet(out, index=False, engine="pyarrow")
    logger.info("Saved cleaned dataset → %s (%d rows)", out, len(df))


def save_spark_output(sdf: SparkDF, name: str, partition_cols: list[str] | None = None) -> None:
    """Persist a Spark DataFrame as Parquet, optionally partitioned.

    Args:
        sdf: Spark DataFrame to write.
        name: Dataset name used as sub-directory under PROCESSED_DATA_PATH.
        partition_cols: Optional partition column names.
    """
    out = str(settings.PROCESSED_DATA_PATH / "spark" / name)
    writer = sdf.write.mode("overwrite").format("parquet")
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)
    writer.save(out)
    logger.info("Saved Spark output → %s (partitions: %s)", out, partition_cols)


# ─────────────────────────────────────────────────────────────────────────────
# Orchestration
# ─────────────────────────────────────────────────────────────────────────────


def run() -> None:
    """Execute the full Silver transformation pipeline."""
    logger.info("=== Silver transformation started ===")

    raw = settings.RAW_DATA_PATH

    # ── Pandas cleaning ─────────────────────────────────────────────────────
    cleaners = {
        "customers": (clean_customers, pd.read_csv(raw / "customers.csv")),
        "products": (clean_products, pd.read_csv(raw / "products.csv")),
        "orders": (clean_orders, pd.read_csv(raw / "orders.csv")),
        "order_items": (clean_order_items, pd.read_csv(raw / "order_items.csv")),
        "payments": (clean_payments, pd.read_csv(raw / "payments.csv")),
        "reviews": (clean_reviews, pd.read_csv(raw / "reviews.csv")),
    }

    for name, (fn, df) in cleaners.items():
        cleaned = fn(df)
        save_cleaned_pandas(cleaned, name)

    # ── PySpark enrichment ──────────────────────────────────────────────────
    spark = _get_spark()
    try:
        outputs = spark_transform(spark)
        partition_map = {
            "orders_enriched": ["year", "month"],
            "monthly_revenue": ["year", "month"],
            "customer_spend": None,
            "items_enriched": None,
            "product_ratings": None,
        }
        for name, sdf in outputs.items():
            save_spark_output(sdf, name, partition_map.get(name))
    finally:
        spark.stop()

    logger.info("=== Silver transformation complete ===")


if __name__ == "__main__":
    run()
