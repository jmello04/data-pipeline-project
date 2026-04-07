"""
pipeline/warehouse/dw_model.py

Gold layer: dimensional modelling.

Builds a star schema from the Silver-layer data:
    - dim_clientes   – customer dimension (SCD Type 1)
    - dim_produtos   – product dimension
    - dim_tempo      – date/time dimension
    - fato_pedidos   – order fact table with surrogate keys

All tables are written as partitioned Parquet to data/analytics/.
"""

import logging
from datetime import date, timedelta
from pathlib import Path

import pandas as pd

from config.settings import settings

# ── Logging ────────────────────────────────────────────────────────────────────
settings.LOGS_PATH.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s – %(message)s",
    handlers=[
        logging.FileHandler(settings.LOGS_PATH / "warehouse.log"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)

_ANALYTICS = settings.ANALYTICS_DATA_PATH


# ─────────────────────────────────────────────────────────────────────────────
# Surrogate key helpers
# ─────────────────────────────────────────────────────────────────────────────


def _add_surrogate_key(df: pd.DataFrame, col_name: str = "sk") -> pd.DataFrame:
    """Prepend a sequential integer surrogate key column.

    Args:
        df: Input DataFrame.
        col_name: Name for the new surrogate key column.

    Returns:
        DataFrame with the surrogate key as the first column.
    """
    df = df.copy()
    df.insert(0, col_name, range(1, len(df) + 1))
    return df


# ─────────────────────────────────────────────────────────────────────────────
# Dimension builders
# ─────────────────────────────────────────────────────────────────────────────


def build_dim_clientes(customers_df: pd.DataFrame) -> pd.DataFrame:
    """Build the customer dimension table.

    Args:
        customers_df: Cleaned/anonymised customers DataFrame from Silver layer.

    Returns:
        dim_clientes DataFrame with surrogate key sk_cliente.
    """
    logger.info("Building dim_clientes …")
    dim = customers_df[["customer_id", "city", "state", "created_at"]].copy()
    dim = dim.rename(columns={"customer_id": "nk_customer_id", "created_at": "dt_criacao"})
    dim = _add_surrogate_key(dim, "sk_cliente")
    logger.info("dim_clientes: %d rows", len(dim))
    return dim


def build_dim_produtos(products_df: pd.DataFrame) -> pd.DataFrame:
    """Build the product dimension table.

    Args:
        products_df: Cleaned products DataFrame from Silver layer.

    Returns:
        dim_produtos DataFrame with surrogate key sk_produto.
    """
    logger.info("Building dim_produtos …")
    dim = products_df[["product_id", "name", "category", "subcategory", "price", "cost"]].copy()
    dim = dim.rename(columns={"product_id": "nk_product_id", "name": "nm_produto"})
    dim["margem_pct"] = ((dim["price"] - dim["cost"]) / dim["price"] * 100).round(2)
    dim = _add_surrogate_key(dim, "sk_produto")
    logger.info("dim_produtos: %d rows", len(dim))
    return dim


def build_dim_tempo(start: date, end: date) -> pd.DataFrame:
    """Build a fully-populated date dimension spanning [start, end].

    Args:
        start: First date in the dimension.
        end: Last date in the dimension (inclusive).

    Returns:
        dim_tempo DataFrame with surrogate key sk_tempo.
    """
    logger.info("Building dim_tempo from %s to %s …", start, end)
    days = pd.date_range(start=start, end=end, freq="D")
    dim = pd.DataFrame(
        {
            "dt_data": days,
            "ano": days.year,
            "mes": days.month,
            "dia": days.day,
            "trimestre": days.quarter,
            "semana_ano": days.isocalendar().week.astype(int),
            "dia_semana": days.day_of_week,
            "nm_dia_semana": days.day_name(),
            "nm_mes": days.month_name(),
            "fl_fimdesemana": days.day_of_week >= 5,
        }
    )
    dim = _add_surrogate_key(dim, "sk_tempo")
    logger.info("dim_tempo: %d rows", len(dim))
    return dim


# ─────────────────────────────────────────────────────────────────────────────
# Fact builder
# ─────────────────────────────────────────────────────────────────────────────


def build_fato_pedidos(
    orders_df: pd.DataFrame,
    items_df: pd.DataFrame,
    payments_df: pd.DataFrame,
    dim_clientes: pd.DataFrame,
    dim_produtos: pd.DataFrame,
    dim_tempo: pd.DataFrame,
) -> pd.DataFrame:
    """Build the orders fact table with surrogate key lookups.

    Args:
        orders_df: Cleaned orders from Silver layer.
        items_df: Cleaned order_items from Silver layer.
        payments_df: Cleaned payments from Silver layer.
        dim_clientes: Customer dimension DataFrame.
        dim_produtos: Product dimension DataFrame.
        dim_tempo: Date dimension DataFrame.

    Returns:
        fato_pedidos grain-level DataFrame (one row per order-item).
        Includes year/month partition columns.
    """
    logger.info("Building fato_pedidos …")

    orders = orders_df.copy()
    orders["order_date"] = pd.to_datetime(orders["order_date"], errors="coerce")
    orders["dt_data"] = orders["order_date"].dt.normalize()

    # ── Surrogate key for customers ─────────────────────────────────────────
    sk_cliente_map = dim_clientes.set_index("nk_customer_id")["sk_cliente"]
    orders["sk_cliente"] = orders["customer_id"].map(sk_cliente_map).fillna(-1).astype(int)

    # ── Surrogate key for date ───────────────────────────────────────────────
    sk_tempo_map = dim_tempo.set_index("dt_data")["sk_tempo"]
    orders["sk_tempo"] = orders["dt_data"].map(sk_tempo_map).fillna(-1).astype(int)

    # ── Join payments ────────────────────────────────────────────────────────
    payments_slim = payments_df[["order_id", "method", "status"]].rename(
        columns={"status": "payment_status"}
    )
    orders = orders.merge(payments_slim, on="order_id", how="left")

    # ── Join items + product SKs ─────────────────────────────────────────────
    items = items_df.copy()
    sk_produto_map = dim_produtos.set_index("nk_product_id")["sk_produto"]
    items["sk_produto"] = items["product_id"].map(sk_produto_map).fillna(-1).astype(int)

    fato = orders.merge(items[["order_id", "item_id", "sk_produto", "quantity", "unit_price",
                                "discount", "line_total"]], on="order_id", how="left")

    # ── Select and rename final columns ─────────────────────────────────────
    fato = fato.rename(columns={
        "order_id": "nk_order_id",
        "total_amount": "vl_total_pedido",
        "shipping_fee": "vl_frete",
        "status": "ds_status",
        "channel": "ds_canal",
    })

    fato["year"] = orders["order_date"].dt.year
    fato["month"] = orders["order_date"].dt.month

    cols = [
        "nk_order_id", "item_id", "sk_cliente", "sk_produto", "sk_tempo",
        "ds_status", "ds_canal", "payment_status", "method",
        "quantity", "unit_price", "discount", "line_total",
        "vl_total_pedido", "vl_frete", "year", "month",
    ]
    fato = fato[[c for c in cols if c in fato.columns]]
    logger.info("fato_pedidos: %d rows", len(fato))
    return fato


# ─────────────────────────────────────────────────────────────────────────────
# Persistence
# ─────────────────────────────────────────────────────────────────────────────


def _save(df: pd.DataFrame, name: str, partition_cols: list[str] | None = None) -> None:
    """Write a DataFrame to the analytics layer as Parquet.

    Args:
        df: DataFrame to persist.
        name: Sub-directory name under ANALYTICS_DATA_PATH.
        partition_cols: Optional Hive-style partition columns.
    """
    out = _ANALYTICS / name
    out.mkdir(parents=True, exist_ok=True)
    df.to_parquet(out, partition_cols=partition_cols, index=False, engine="pyarrow")
    logger.info("Saved %s → %s (partitions: %s)", name, out, partition_cols)


# ─────────────────────────────────────────────────────────────────────────────
# Orchestration
# ─────────────────────────────────────────────────────────────────────────────


def run() -> None:
    """Execute the full Gold dimensional modelling pipeline."""
    logger.info("=== Gold layer (dimensional modelling) started ===")

    cleaned = settings.PROCESSED_DATA_PATH / "cleaned"

    customers_df = pd.read_parquet(cleaned / "customers")
    products_df = pd.read_parquet(cleaned / "products")
    orders_df = pd.read_parquet(cleaned / "orders")
    items_df = pd.read_parquet(cleaned / "order_items")
    payments_df = pd.read_parquet(cleaned / "payments")

    # ── Dimensions ───────────────────────────────────────────────────────────
    dim_clientes = build_dim_clientes(customers_df)
    dim_produtos = build_dim_produtos(products_df)

    orders_df["order_date"] = pd.to_datetime(orders_df["order_date"], errors="coerce")
    date_min = orders_df["order_date"].min().date()
    date_max = orders_df["order_date"].max().date()
    dim_tempo = build_dim_tempo(date_min, date_max)

    # ── Fact ─────────────────────────────────────────────────────────────────
    fato_pedidos = build_fato_pedidos(
        orders_df, items_df, payments_df,
        dim_clientes, dim_produtos, dim_tempo,
    )

    # ── Persist ──────────────────────────────────────────────────────────────
    _save(dim_clientes, "dim_clientes")
    _save(dim_produtos, "dim_produtos")
    _save(dim_tempo, "dim_tempo")
    _save(fato_pedidos, "fato_pedidos", partition_cols=["year", "month"])

    logger.info("=== Gold layer complete ===")


if __name__ == "__main__":
    run()
