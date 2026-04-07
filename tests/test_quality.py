"""
tests/test_quality.py

Unit tests for quality_checks and transform modules.
Run with: pytest tests/ -v
"""

import hashlib
import pytest
import pandas as pd

from pipeline.quality.quality_checks import (
    check_null_rate,
    check_numeric_range,
    check_referential_integrity,
    check_duplicates,
)
from pipeline.transformation.transform import (
    _sha256,
    clean_customers,
    clean_products,
    clean_orders,
    clean_order_items,
)


# ─────────────────────────────────────────────────────────────────────────────
# Fixtures
# ─────────────────────────────────────────────────────────────────────────────


@pytest.fixture()
def clean_customers_df() -> pd.DataFrame:
    """Minimal valid customers DataFrame."""
    return pd.DataFrame(
        {
            "customer_id": [1, 2, 3],
            "name": ["Alice", "Bob", "Carol"],
            "email": ["a@x.com", "b@x.com", "c@x.com"],
            "phone": ["11999", "11888", "11777"],
            "city": ["SP", "RJ", "MG"],
            "state": ["SP", "RJ", "MG"],
            "birth_date": ["1990-01-01", "1985-06-15", "2000-03-20"],
            "created_at": ["2020-01-01", "2021-03-10", "2022-07-05"],
        }
    )


@pytest.fixture()
def clean_products_df() -> pd.DataFrame:
    """Minimal valid products DataFrame."""
    return pd.DataFrame(
        {
            "product_id": [1, 2, 3],
            "name": ["Widget A", "Gadget B", "Tool C"],
            "category": ["Electronics", "Electronics", "Home"],
            "subcategory": ["Accessories", "Laptops", "Kitchen"],
            "price": [99.99, 1499.0, 49.5],
            "cost": [40.0, 600.0, 20.0],
            "stock_qty": [100, 50, 200],
            "created_at": ["2021-01-01", "2021-06-01", "2022-01-15"],
        }
    )


@pytest.fixture()
def orders_df_with_customer_ref(clean_customers_df: pd.DataFrame) -> pd.DataFrame:
    """Orders referencing customers in clean_customers_df."""
    return pd.DataFrame(
        {
            "order_id": [101, 102, 103],
            "customer_id": [1, 2, 3],
            "order_date": ["2023-01-10", "2023-02-14", "2023-03-01"],
            "status": ["delivered", "shipped", "cancelled"],
            "total_amount": [150.0, 2000.0, 50.0],
            "shipping_fee": [15.0, 0.0, 10.0],
            "channel": ["web", "mobile", "web"],
        }
    )


# ─────────────────────────────────────────────────────────────────────────────
# SHA-256 helper
# ─────────────────────────────────────────────────────────────────────────────


class TestSha256:
    def test_returns_64_char_hex(self) -> None:
        result = _sha256("test_value")
        assert len(result) == 64
        assert all(c in "0123456789abcdef" for c in result)

    def test_deterministic(self) -> None:
        assert _sha256("hello") == _sha256("hello")

    def test_different_inputs_produce_different_hashes(self) -> None:
        assert _sha256("alice@example.com") != _sha256("bob@example.com")

    def test_matches_stdlib_sha256(self) -> None:
        plain = "data_engineering"
        expected = hashlib.sha256(plain.encode()).hexdigest()
        assert _sha256(plain) == expected


# ─────────────────────────────────────────────────────────────────────────────
# check_null_rate
# ─────────────────────────────────────────────────────────────────────────────


class TestCheckNullRate:
    def test_passes_when_no_nulls(self, clean_customers_df: pd.DataFrame) -> None:
        result = check_null_rate(clean_customers_df, "customers", threshold=0.10)
        assert result["status"] == "PASS"
        assert result["error_count"] == 0

    def test_fails_when_null_exceeds_threshold(self) -> None:
        df = pd.DataFrame({"a": [None, None, None, 1, 2], "b": [1, 2, 3, 4, 5]})
        result = check_null_rate(df, "test_df", threshold=0.10)
        assert result["status"] == "FAIL"
        assert result["error_count"] >= 1
        assert "a" in result["details"]

    def test_passes_when_null_at_exact_threshold(self) -> None:
        df = pd.DataFrame({"a": [None, 1, 2, 3, 4, 5, 6, 7, 8, 9]})
        result = check_null_rate(df, "edge_df", threshold=0.10)
        # 1/10 = 0.10 — not strictly above threshold → PASS
        assert result["status"] == "PASS"

    def test_result_contains_required_keys(self, clean_customers_df: pd.DataFrame) -> None:
        result = check_null_rate(clean_customers_df, "customers")
        for key in ("check", "dataset", "status", "error_count", "details", "checked_at"):
            assert key in result


# ─────────────────────────────────────────────────────────────────────────────
# check_numeric_range
# ─────────────────────────────────────────────────────────────────────────────


class TestCheckNumericRange:
    def test_passes_when_all_values_in_range(self, clean_products_df: pd.DataFrame) -> None:
        result = check_numeric_range(clean_products_df, "products", "price", min_val=0)
        assert result["status"] == "PASS"
        assert result["error_count"] == 0

    def test_fails_when_negative_price_present(self) -> None:
        df = pd.DataFrame({"price": [10.0, -5.0, 20.0]})
        result = check_numeric_range(df, "test", "price", min_val=0)
        assert result["status"] == "FAIL"
        assert result["error_count"] == 1

    def test_fails_when_rating_out_of_1_5_range(self) -> None:
        df = pd.DataFrame({"rating": [1, 3, 5, 6, 0]})
        result = check_numeric_range(df, "reviews", "rating", min_val=1, max_val=5)
        assert result["status"] == "FAIL"
        assert result["error_count"] == 2

    def test_passes_with_no_bounds(self, clean_products_df: pd.DataFrame) -> None:
        result = check_numeric_range(clean_products_df, "products", "price")
        assert result["status"] == "PASS"


# ─────────────────────────────────────────────────────────────────────────────
# check_referential_integrity
# ─────────────────────────────────────────────────────────────────────────────


class TestCheckReferentialIntegrity:
    def test_passes_when_all_fk_exist(
        self,
        orders_df_with_customer_ref: pd.DataFrame,
        clean_customers_df: pd.DataFrame,
    ) -> None:
        result = check_referential_integrity(
            orders_df_with_customer_ref, clean_customers_df,
            "customer_id", "customer_id", "orders", "customers",
        )
        assert result["status"] == "PASS"
        assert result["error_count"] == 0

    def test_fails_when_orphan_fk_present(
        self, clean_customers_df: pd.DataFrame
    ) -> None:
        orders_orphan = pd.DataFrame(
            {
                "order_id": [201],
                "customer_id": [9999],  # does not exist
                "total_amount": [50.0],
            }
        )
        result = check_referential_integrity(
            orders_orphan, clean_customers_df,
            "customer_id", "customer_id", "orders", "customers",
        )
        assert result["status"] == "FAIL"
        assert result["error_count"] == 1
        assert "9999" in result["details"]["sample_orphans"]


# ─────────────────────────────────────────────────────────────────────────────
# check_duplicates
# ─────────────────────────────────────────────────────────────────────────────


class TestCheckDuplicates:
    def test_passes_when_pk_is_unique(self, clean_customers_df: pd.DataFrame) -> None:
        result = check_duplicates(clean_customers_df, "customers", "customer_id")
        assert result["status"] == "PASS"
        assert result["error_count"] == 0

    def test_fails_when_pk_has_duplicates(self) -> None:
        df = pd.DataFrame({"id": [1, 2, 2, 3], "val": ["a", "b", "c", "d"]})
        result = check_duplicates(df, "test", "id")
        assert result["status"] == "FAIL"
        assert result["error_count"] == 1


# ─────────────────────────────────────────────────────────────────────────────
# clean_customers (LGPD + typing)
# ─────────────────────────────────────────────────────────────────────────────


class TestCleanCustomers:
    def test_pii_columns_are_hashed(self, clean_customers_df: pd.DataFrame) -> None:
        result = clean_customers(clean_customers_df)
        # Original plain values must no longer appear
        assert "Alice" not in result["name"].values
        assert "a@x.com" not in result["email"].values

    def test_hashes_are_64_chars(self, clean_customers_df: pd.DataFrame) -> None:
        result = clean_customers(clean_customers_df)
        assert result["customer_id"].str.len().eq(64).all()
        assert result["name"].str.len().eq(64).all()
        assert result["email"].str.len().eq(64).all()

    def test_drops_rows_with_null_customer_id(self) -> None:
        df = pd.DataFrame(
            {
                "customer_id": [None, 2],
                "name": ["X", "Y"],
                "email": ["x@x.com", "y@x.com"],
                "phone": ["11", "22"],
                "city": ["A", "B"],
                "state": ["SP", "RJ"],
                "birth_date": ["1990-01-01", "1985-01-01"],
                "created_at": ["2020-01-01", "2021-01-01"],
            }
        )
        result = clean_customers(df)
        assert len(result) == 1

    def test_created_at_is_datetime(self, clean_customers_df: pd.DataFrame) -> None:
        result = clean_customers(clean_customers_df)
        assert pd.api.types.is_datetime64_any_dtype(result["created_at"])


# ─────────────────────────────────────────────────────────────────────────────
# clean_products
# ─────────────────────────────────────────────────────────────────────────────


class TestCleanProducts:
    def test_stock_qty_nulls_filled_with_zero(self) -> None:
        df = pd.DataFrame(
            {
                "product_id": [1, 2],
                "name": ["A", "B"],
                "category": ["X", "Y"],
                "subcategory": ["x", "y"],
                "price": [10.0, 20.0],
                "cost": [5.0, 8.0],
                "stock_qty": [None, 10],
                "created_at": ["2021-01-01", "2021-01-01"],
            }
        )
        result = clean_products(df)
        assert result.loc[result["product_id"] == 1, "stock_qty"].iloc[0] == 0

    def test_duplicate_product_ids_removed(self) -> None:
        df = pd.DataFrame(
            {
                "product_id": [1, 1, 2],
                "name": ["A", "A_dup", "B"],
                "category": ["X", "X", "Y"],
                "subcategory": ["x", "x", "y"],
                "price": [10.0, 10.0, 20.0],
                "cost": [5.0, 5.0, 8.0],
                "stock_qty": [5, 5, 10],
                "created_at": ["2021-01-01", "2021-01-01", "2022-01-01"],
            }
        )
        result = clean_products(df)
        assert len(result) == 2


# ─────────────────────────────────────────────────────────────────────────────
# clean_order_items
# ─────────────────────────────────────────────────────────────────────────────


class TestCleanOrderItems:
    def test_discount_clipped_to_zero_one(self) -> None:
        df = pd.DataFrame(
            {
                "item_id": [1, 2, 3],
                "order_id": [10, 11, 12],
                "product_id": [1, 2, 3],
                "quantity": [1, 2, 1],
                "unit_price": [50.0, 100.0, 25.0],
                "discount": [-0.1, 0.5, 1.5],  # out-of-range values
                "line_total": [50.0, 90.0, 25.0],
            }
        )
        result = clean_order_items(df)
        assert (result["discount"] >= 0).all()
        assert (result["discount"] <= 1).all()
