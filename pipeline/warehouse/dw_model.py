"""
pipeline/warehouse/dw_model.py

Gold layer: dimensional modelling (star schema).

Design decisions:
    - Surrogate keys are derived deterministically from the natural key via
      CRC32.  Sequential integers (range(1, n+1)) are non-idempotent: the
      same business entity gets a different SK if the DataFrame sort order
      changes between runs.  CRC32(nk) is stable, fast, and collision-
      resistant for datasets up to ~10^4 rows (birthday bound ~sqrt(2^32)).
    - Margin (margem_pct) is only computed when price > 0.  price = 0 rows
      get margem_pct = 0.0 instead of ZeroDivisionError.
    - Orphan surrogate key lookups (-1 sentinel) are counted and logged as
      warnings, not silently absorbed into the fact table.
    - Fact table column list is enforced with an explicit contract; missing
      columns raise immediately.
"""

import binascii
import logging
from datetime import date

import pandas as pd

from config.settings import settings
from pipeline.utils.decorators import timed
from pipeline.utils.logging_config import get_logger

logger = get_logger(__name__, log_file=settings.LOGS_PATH / "warehouse.log")

_ANALYTICS = settings.ANALYTICS_DATA_PATH

# ─────────────────────────────────────────────────────────────────────────────
# Surrogate key helpers
# ─────────────────────────────────────────────────────────────────────────────

def _crc32_sk(value: object) -> int:
    """Derive a deterministic, positive surrogate key via CRC32.

    CRC32 is stable across runs — the same natural key always maps to the
    same surrogate key regardless of DataFrame order or execution environment.

    Args:
        value: Natural key value.  Converted to string before hashing.

    Returns:
        Positive 32-bit integer surrogate key.
    """
    crc = binascii.crc32(str(value).encode()) & 0xFFFFFFFF
    # 0 is the sentinel for "unknown member" in the fact table.
    # If CRC32 happens to produce 0, shift to 1 to keep 0 free.
    return int(crc) if crc != 0 else 1


def _build_sk_column(series: pd.Series, col_name: str) -> pd.Series:
    """Compute surrogate keys for every natural key in a Series.

    Args:
        series: Natural key Series.
        col_name: Used only in logging.

    Returns:
        Integer Series of surrogate keys.
    """
    sk = series.map(_crc32_sk).astype("int64")
    dupes = int(sk.duplicated().sum())
    if dupes > 0:
        logger.warning(
            "CRC32 collision detected in SK column — consider a wider hash",
            extra={"column": col_name, "collisions": dupes},
        )
    return sk


# ─────────────────────────────────────────────────────────────────────────────
# Dimension builders
# ─────────────────────────────────────────────────────────────────────────────

@timed
def build_dim_clientes(customers_df: pd.DataFrame) -> pd.DataFrame:
    """Build the customer dimension table.

    Args:
        customers_df: Cleaned/anonymised customers DataFrame from Silver layer.

    Returns:
        dim_clientes DataFrame with deterministic surrogate key sk_cliente.
    """
    logger.info("Building dim_clientes")
    dim = customers_df[["customer_id", "city", "state", "created_at"]].copy()
    dim = dim.rename(columns={"customer_id": "nk_customer_id", "created_at": "dt_criacao"})
    dim.insert(0, "sk_cliente", _build_sk_column(dim["nk_customer_id"], "sk_cliente"))
    logger.info("dim_clientes built", extra={"rows": len(dim)})
    return dim


@timed
def build_dim_produtos(products_df: pd.DataFrame) -> pd.DataFrame:
    """Build the product dimension table.

    margem_pct is only computed when price > 0 to avoid ZeroDivisionError.

    Args:
        products_df: Cleaned products DataFrame from Silver layer.

    Returns:
        dim_produtos DataFrame with deterministic surrogate key sk_produto.
    """
    logger.info("Building dim_produtos")
    dim = products_df[["product_id", "name", "category", "subcategory", "price", "cost"]].copy()
    dim = dim.rename(columns={"product_id": "nk_product_id", "name": "nm_produto"})

    # Safe margin calculation — price = 0 rows get margin = 0.0
    safe_price = dim["price"].replace(0, pd.NA)
    dim["margem_pct"] = ((safe_price - dim["cost"]) / safe_price * 100).fillna(0.0).round(2)

    dim.insert(0, "sk_produto", _build_sk_column(dim["nk_product_id"], "sk_produto"))
    logger.info("dim_produtos built", extra={"rows": len(dim)})
    return dim


@timed
def build_dim_tempo(start: date, end: date) -> pd.DataFrame:
    """Build a fully-populated date dimension spanning [start, end].

    Args:
        start: First date in the dimension.
        end: Last date in the dimension (inclusive).

    Returns:
        dim_tempo with deterministic surrogate key sk_tempo.

    Raises:
        ValueError: If start > end.
    """
    if start > end:
        raise ValueError(f"start ({start}) must be ≤ end ({end})")

    logger.info("Building dim_tempo", extra={"start": str(start), "end": str(end)})
    days = pd.date_range(start=start, end=end, freq="D")
    iso_dates = days.strftime("%Y-%m-%d")  # stable string key for CRC32

    dim = pd.DataFrame({
        "dt_data":       days.date,
        "ano":           days.year,
        "mes":           days.month,
        "dia":           days.day,
        "trimestre":     days.quarter,
        "semana_ano":    days.isocalendar().week.astype(int).values,
        "dia_semana":    days.day_of_week,
        "nm_dia_semana": days.day_name(),
        "nm_mes":        days.month_name(),
        "fl_fimdesemana": (days.day_of_week >= 5),
    })
    dim.insert(0, "sk_tempo", pd.Series(iso_dates).map(_crc32_sk).values)
    logger.info("dim_tempo built", extra={"rows": len(dim)})
    return dim


# ─────────────────────────────────────────────────────────────────────────────
# Fact builder
# ─────────────────────────────────────────────────────────────────────────────

# Grain: one row per order-item line.
_FATO_COLS = [
    "nk_order_id", "item_id", "sk_cliente", "sk_produto", "sk_tempo",
    "ds_status", "ds_canal", "payment_status", "method",
    "quantity", "unit_price", "discount", "line_total",
    "vl_total_pedido", "vl_frete", "year", "month",
]


@timed
def build_fato_pedidos(
    orders_df: pd.DataFrame,
    items_df: pd.DataFrame,
    payments_df: pd.DataFrame,
    dim_clientes: pd.DataFrame,
    dim_produtos: pd.DataFrame,
    dim_tempo: pd.DataFrame,
) -> pd.DataFrame:
    """Build the order fact table with surrogate key lookups.

    Grain: one row per order-item (order_id × item_id).

    Missing lookups receive SK = 0 (unknown member) and are logged as
    warnings — not silently absorbed.

    Args:
        orders_df: Cleaned orders from Silver layer.
        items_df: Cleaned order_items from Silver layer.
        payments_df: Cleaned payments from Silver layer.
        dim_clientes: Customer dimension.
        dim_produtos: Product dimension.
        dim_tempo: Date dimension.

    Returns:
        fato_pedidos DataFrame partitioned-ready (year, month columns present).

    Raises:
        KeyError: If a required fact column is missing after join.
    """
    logger.info("Building fato_pedidos")

    orders = orders_df.copy()
    orders["order_date"] = pd.to_datetime(orders["order_date"], errors="coerce")
    orders["dt_data"]    = orders["order_date"].dt.normalize()

    # ── Customer SK lookup ─────────────────────────────────────────────────
    sk_cli_map = dict(zip(dim_clientes["nk_customer_id"], dim_clientes["sk_cliente"]))
    orders["sk_cliente"] = orders["customer_id"].map(sk_cli_map).fillna(0).astype("int64")
    n_orphan_cli = int((orders["sk_cliente"] == 0).sum())
    if n_orphan_cli:
        logger.warning("Orphan customer keys in fato", extra={"count": n_orphan_cli})

    # ── Date SK lookup ─────────────────────────────────────────────────────
    dim_tempo_cp = dim_tempo.copy()
    dim_tempo_cp["dt_data"] = pd.to_datetime(dim_tempo_cp["dt_data"])
    sk_dt_map = dict(zip(dim_tempo_cp["dt_data"], dim_tempo_cp["sk_tempo"]))
    orders["sk_tempo"] = orders["dt_data"].map(sk_dt_map).fillna(0).astype("int64")
    n_orphan_dt = int((orders["sk_tempo"] == 0).sum())
    if n_orphan_dt:
        logger.warning("Orphan date keys in fato", extra={"count": n_orphan_dt})

    # ── Payment join ───────────────────────────────────────────────────────
    pay_slim = payments_df[["order_id", "method", "status"]].rename(
        columns={"status": "payment_status"}
    )
    orders = orders.merge(pay_slim, on="order_id", how="left")

    # ── Item join + product SK lookup ──────────────────────────────────────
    items = items_df.copy()
    sk_prod_map = dict(zip(dim_produtos["nk_product_id"], dim_produtos["sk_produto"]))
    items["sk_produto"] = items["product_id"].map(sk_prod_map).fillna(0).astype("int64")
    n_orphan_prod = int((items["sk_produto"] == 0).sum())
    if n_orphan_prod:
        logger.warning("Orphan product keys in fato", extra={"count": n_orphan_prod})

    # Build year/month map from orders BEFORE the one-to-many item join.
    # After the join fato has more rows than orders (one row per item line),
    # so slicing orders["year"].values[:len(fato)] would give wrong results.
    ym_map = orders.set_index("order_id").assign(
        year=lambda df: df["order_date"].dt.year,
        month=lambda df: df["order_date"].dt.month,
    )[["year", "month"]]

    fato = orders.merge(
        items[["order_id", "item_id", "sk_produto", "quantity", "unit_price", "discount", "line_total"]],
        on="order_id",
        how="left",
    )

    fato = fato.rename(columns={
        "order_id":     "nk_order_id",
        "total_amount": "vl_total_pedido",
        "shipping_fee": "vl_frete",
        "status":       "ds_status",
        "channel":      "ds_canal",
    })

    # Map year/month by nk_order_id — correct for any number of items per order.
    fato["year"]  = fato["nk_order_id"].map(ym_map["year"])
    fato["month"] = fato["nk_order_id"].map(ym_map["month"])

    # Enforce contract: all required columns must be present
    missing = [c for c in _FATO_COLS if c not in fato.columns]
    if missing:
        raise KeyError(f"fato_pedidos is missing required columns: {missing}")

    fato = fato[_FATO_COLS]
    logger.info("fato_pedidos built", extra={"rows": len(fato)})
    return fato


# ─────────────────────────────────────────────────────────────────────────────
# Persistence
# ─────────────────────────────────────────────────────────────────────────────

def _save(df: pd.DataFrame, name: str, partition_cols: list[str] | None = None) -> None:
    """Write a Gold-layer DataFrame as Parquet.

    Args:
        df: DataFrame to persist.
        name: Sub-directory under ANALYTICS_DATA_PATH.
        partition_cols: Optional Hive-style partition columns.
    """
    out = _ANALYTICS / name
    out.mkdir(parents=True, exist_ok=True)
    df.to_parquet(out, partition_cols=partition_cols, index=False, engine="pyarrow")
    logger.info("Saved Gold table", extra={"name": name, "path": str(out), "rows": len(df),
                                           "partitions": partition_cols})


# ─────────────────────────────────────────────────────────────────────────────
# Orchestration
# ─────────────────────────────────────────────────────────────────────────────

@timed
def run() -> None:
    """Execute the full Gold dimensional modelling pipeline."""
    logger.info("=== Gold layer started ===")
    cleaned = settings.PROCESSED_DATA_PATH / "cleaned"

    customers_df = pd.read_parquet(cleaned / "customers")
    products_df  = pd.read_parquet(cleaned / "products")
    orders_df    = pd.read_parquet(cleaned / "orders")
    items_df     = pd.read_parquet(cleaned / "order_items")
    payments_df  = pd.read_parquet(cleaned / "payments")

    dim_clientes = build_dim_clientes(customers_df)
    dim_produtos = build_dim_produtos(products_df)

    orders_df["order_date"] = pd.to_datetime(orders_df["order_date"], errors="coerce")
    date_min = orders_df["order_date"].dropna().min().date()
    date_max = orders_df["order_date"].dropna().max().date()
    dim_tempo = build_dim_tempo(date_min, date_max)

    fato = build_fato_pedidos(
        orders_df, items_df, payments_df,
        dim_clientes, dim_produtos, dim_tempo,
    )

    _save(dim_clientes, "dim_clientes")
    _save(dim_produtos, "dim_produtos")
    _save(dim_tempo,    "dim_tempo")
    _save(fato,         "fato_pedidos", partition_cols=["year", "month"])

    logger.info("=== Gold layer complete ===")


if __name__ == "__main__":
    run()
