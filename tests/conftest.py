"""
tests/conftest.py

Shared pytest fixtures.

All fixtures that produce DataFrames matching production schemas use the
LGPD-anonymised form (HMAC-hashed customer_id).  This ensures tests exercise
the same data shape that runs in production — not a idealised plain-text
version that hides real problems.
"""

import os

import pandas as pd
import pytest

# Force-set a deterministic test key for LGPD tests.
# os.environ.setdefault() would silently keep any pre-existing value, which
# could be a production key that changes hash outputs and breaks assertions.
# We always override so test behaviour is fully reproducible.
os.environ["LGPD_HASH_KEY"] = "test-secret-key-for-unit-tests-only-abcdef"

from pipeline.transformation.transform import _anonymise


# ─────────────────────────────────────────────────────────────────────────────
# Shared constants
# ─────────────────────────────────────────────────────────────────────────────

CUSTOMER_IDS = [1, 2, 3]
PRODUCT_IDS  = [10, 20, 30]
ORDER_IDS    = [101, 102, 103]


# ─────────────────────────────────────────────────────────────────────────────
# Base fixtures
# ─────────────────────────────────────────────────────────────────────────────

@pytest.fixture(scope="session")
def hashed_customer_ids() -> list[str]:
    """HMAC-hashed customer IDs (matches production anonymisation).

    Scope is session so the hash is computed once.
    """
    return [_anonymise(cid) for cid in CUSTOMER_IDS]


@pytest.fixture(scope="session")
def customers_df(hashed_customer_ids: list[str]) -> pd.DataFrame:
    """Customer dimension DataFrame with HMAC-anonymised IDs."""
    return pd.DataFrame({
        "customer_id": hashed_customer_ids,
        "name":        [_anonymise(n) for n in ["Alice", "Bob", "Carol"]],
        "email":       [_anonymise(e) for e in ["a@x.com", "b@x.com", "c@x.com"]],
        "phone":       ["11999", "11888", "11777"],
        "city":        ["São Paulo", "Rio de Janeiro", "Belo Horizonte"],
        "state":       ["SP", "RJ", "MG"],
        "birth_date":  ["1990-01-01", "1985-06-15", "2000-03-20"],
        "created_at":  ["2020-01-01", "2021-03-10", "2022-07-05"],
    })


@pytest.fixture(scope="session")
def products_df() -> pd.DataFrame:
    """Product DataFrame with valid price/cost (cost < price)."""
    return pd.DataFrame({
        "product_id":  PRODUCT_IDS,
        "name":        ["Widget A", "Gadget B", "Tool C"],
        "category":    ["Electronics", "Electronics", "Home"],
        "subcategory": ["Accessories", "Laptops", "Kitchen"],
        "price":       [99.99, 1499.00, 49.50],
        "cost":        [40.00,  600.00, 20.00],
        "stock_qty":   [100, 50, 200],
        "created_at":  ["2021-01-01", "2021-06-01", "2022-01-15"],
    })


@pytest.fixture(scope="session")
def orders_df(hashed_customer_ids: list[str]) -> pd.DataFrame:
    """Orders DataFrame referencing HMAC-anonymised customer IDs."""
    return pd.DataFrame({
        "order_id":     ORDER_IDS,
        "customer_id":  hashed_customer_ids,   # must match anonymised customers
        "order_date":   ["2023-01-10", "2023-02-14", "2023-03-01"],
        "status":       ["delivered", "shipped", "cancelled"],
        "total_amount": [150.00, 2000.00, 50.00],
        "shipping_fee": [15.00, 0.00, 10.00],
        "channel":      ["web", "mobile", "web"],
    })


@pytest.fixture(scope="session")
def order_items_df() -> pd.DataFrame:
    """Order items DataFrame."""
    return pd.DataFrame({
        "item_id":    [1, 2, 3, 4],
        "order_id":   [101, 101, 102, 103],
        "product_id": [10, 20, 10, 30],
        "quantity":   [2, 1, 3, 1],
        "unit_price": [99.99, 1499.00, 99.99, 49.50],
        "discount":   [0.0, 0.1, 0.05, 0.0],
        "line_total": [199.98, 1349.10, 284.97, 49.50],
    })
