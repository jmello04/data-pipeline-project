"""
tests/test_quality.py

Unit tests for quality_checks and transform modules.

Strategy:
    - Unit tests cover every check function and every cleaning function.
    - Property-based tests (Hypothesis) verify invariants that hold for
      all valid inputs — not just the three rows in a hand-crafted fixture.
    - Fixtures from conftest.py use HMAC-anonymised IDs so tests exercise
      the same data shape that runs in production.

Run:
    pytest tests/ -v
"""

import hashlib
import hmac
import os

import pandas as pd
import pytest
from hypothesis import given, settings as h_settings
from hypothesis import strategies as st

# Force-set — must match conftest.py so all hash assertions are consistent.
os.environ["LGPD_HASH_KEY"] = "test-secret-key-for-unit-tests-only-abcdef"

from pipeline.quality.quality_checks import (
    check_duplicates,
    check_null_rate,
    check_numeric_range,
    check_referential_integrity,
    check_schema,
)
from pipeline.transformation.transform import (
    _anonymise,
    _anonymise_series,
    clean_customers,
    clean_order_items,
    clean_orders,
    clean_products,
)


# ─────────────────────────────────────────────────────────────────────────────
# _anonymise
# ─────────────────────────────────────────────────────────────────────────────

class TestAnonymise:
    def test_returns_64_char_hex(self) -> None:
        assert len(_anonymise("test")) == 64

    def test_is_lowercase_hex(self) -> None:
        result = _anonymise("hello")
        assert all(c in "0123456789abcdef" for c in result)

    def test_deterministic_same_value(self) -> None:
        assert _anonymise("same") == _anonymise("same")

    def test_different_values_produce_different_digests(self) -> None:
        assert _anonymise("alice@example.com") != _anonymise("bob@example.com")

    def test_none_produces_consistent_sentinel(self) -> None:
        """None must hash to the same value every time (not random)."""
        assert _anonymise(None) == _anonymise(None)
        assert len(_anonymise(None)) == 64

    def test_matches_stdlib_hmac(self) -> None:
        key = os.environ["LGPD_HASH_KEY"].encode()
        expected = hmac.new(key, b"data_engineering", hashlib.sha256).hexdigest()
        assert _anonymise("data_engineering") == expected

    def test_integer_input_handled(self) -> None:
        """Integer customer IDs must hash consistently."""
        assert _anonymise(42) == _anonymise(42)
        assert _anonymise(42) != _anonymise(43)

    @given(st.text(min_size=0, max_size=200))
    @h_settings(max_examples=100)
    def test_always_returns_64_char_hex_for_any_string(self, value: str) -> None:
        """Property: any string input → 64-char hex output."""
        result = _anonymise(value)
        assert len(result) == 64
        assert all(c in "0123456789abcdef" for c in result)

    @given(st.integers())
    @h_settings(max_examples=50)
    def test_integer_always_produces_64_char_hex(self, value: int) -> None:
        """Property: any integer → 64-char hex output."""
        assert len(_anonymise(value)) == 64


# ─────────────────────────────────────────────────────────────────────────────
# check_null_rate
# ─────────────────────────────────────────────────────────────────────────────

class TestCheckNullRate:
    def test_passes_when_no_nulls(self, customers_df: pd.DataFrame) -> None:
        result = check_null_rate(customers_df, "customers", threshold=0.10)
        assert result["status"] == "PASS"
        assert result["error_count"] == 0

    def test_fails_when_null_exceeds_threshold(self) -> None:
        df = pd.DataFrame({"a": [None, None, None, 1, 2], "b": [1, 2, 3, 4, 5]})
        result = check_null_rate(df, "test", threshold=0.10)
        assert result["status"] == "FAIL"
        assert "a" in result["details"]

    def test_passes_when_null_exactly_at_threshold(self) -> None:
        # 1/10 = 0.10 — NOT strictly above threshold → PASS
        df = pd.DataFrame({"a": [None, 1, 2, 3, 4, 5, 6, 7, 8, 9]})
        assert check_null_rate(df, "edge", threshold=0.10)["status"] == "PASS"

    def test_fails_when_null_just_above_threshold(self) -> None:
        # 2/10 = 0.20 > 0.10 → FAIL
        df = pd.DataFrame({"a": [None, None, 1, 2, 3, 4, 5, 6, 7, 8]})
        assert check_null_rate(df, "just_above", threshold=0.10)["status"] == "FAIL"

    def test_result_contains_required_keys(self, customers_df: pd.DataFrame) -> None:
        result = check_null_rate(customers_df, "customers")
        for key in ("check", "dataset", "status", "error_count", "details", "checked_at"):
            assert key in result

    @given(st.floats(min_value=0.0, max_value=1.0, allow_nan=False))
    @h_settings(max_examples=30)
    def test_threshold_in_range_never_raises(self, threshold: float) -> None:
        """Property: any valid threshold → no exception."""
        df = pd.DataFrame({"x": [1, 2, None]})
        result = check_null_rate(df, "prop_test", threshold=threshold)
        assert result["status"] in ("PASS", "FAIL")


# ─────────────────────────────────────────────────────────────────────────────
# check_numeric_range
# ─────────────────────────────────────────────────────────────────────────────

@pytest.mark.parametrize("values,min_val,max_val,expected_status", [
    ([10.0, 20.0, 30.0],   0,    None,  "PASS"),
    ([10.0, -5.0, 20.0],   0,    None,  "FAIL"),
    ([1, 3, 5],             1,    5,     "PASS"),
    ([1, 3, 5, 6],          1,    5,     "FAIL"),
    ([1, 3, 5, 0],          1,    5,     "FAIL"),
    ([0.0, 0.5, 1.0],       0,    1,     "PASS"),
    ([0.0, 0.5, 1.01],      0,    1,     "FAIL"),
])
def test_check_numeric_range_parametrised(
    values: list, min_val: float | None, max_val: float | None, expected_status: str
) -> None:
    df = pd.DataFrame({"v": values})
    assert check_numeric_range(df, "ds", "v", min_val=min_val, max_val=max_val)["status"] == expected_status


def test_check_numeric_range_missing_column_raises() -> None:
    df = pd.DataFrame({"a": [1, 2]})
    with pytest.raises(ValueError, match="Missing required columns"):
        check_numeric_range(df, "ds", "nonexistent")


# ─────────────────────────────────────────────────────────────────────────────
# check_referential_integrity
# ─────────────────────────────────────────────────────────────────────────────

class TestCheckReferentialIntegrity:
    def test_passes_when_all_fk_exist(
        self, orders_df: pd.DataFrame, customers_df: pd.DataFrame
    ) -> None:
        """Hashed FK in orders must match hashed PK in customers."""
        result = check_referential_integrity(
            orders_df, customers_df,
            "customer_id", "customer_id", "orders", "customers",
        )
        assert result["status"] == "PASS"
        assert result["error_count"] == 0

    def test_fails_on_orphan_fk(self, customers_df: pd.DataFrame) -> None:
        orphan_hash = _anonymise(99999)  # does not exist in customers_df
        orders_orphan = pd.DataFrame({
            "order_id":    [999],
            "customer_id": [orphan_hash],
        })
        result = check_referential_integrity(
            orders_orphan, customers_df,
            "customer_id", "customer_id", "orders", "customers",
        )
        assert result["status"] == "FAIL"
        assert result["error_count"] >= 1
        assert orphan_hash in result["details"]["sample_orphans"]

    def test_counts_null_fks_as_violations(self, customers_df: pd.DataFrame) -> None:
        df_with_null = pd.DataFrame({"order_id": [1, 2], "customer_id": [None, None]})
        result = check_referential_integrity(
            df_with_null, customers_df,
            "customer_id", "customer_id", "orders", "customers",
        )
        assert result["status"] == "FAIL"
        assert result["details"]["null_fk_count"] == 2

    def test_type_mismatch_does_not_cause_false_pass(self) -> None:
        """Integer FK vs string PK — different types, comparison uses native types."""
        parent = pd.DataFrame({"id": ["1", "2", "3"]})   # string PKs
        child  = pd.DataFrame({"fk": [1, 2, 3]})          # int FKs — won't match
        result = check_referential_integrity(child, parent, "fk", "id", "child", "parent")
        # int 1 ≠ str "1" — this is correct behaviour (caller must ensure type consistency)
        assert result["status"] == "FAIL"


# ─────────────────────────────────────────────────────────────────────────────
# check_duplicates
# ─────────────────────────────────────────────────────────────────────────────

@pytest.mark.parametrize("ids,expected_status,expected_count", [
    ([1, 2, 3],       "PASS", 0),
    ([1, 2, 2],       "FAIL", 1),
    ([1, 1, 1],       "FAIL", 2),
    ([],              "PASS", 0),
])
def test_check_duplicates_parametrised(
    ids: list, expected_status: str, expected_count: int
) -> None:
    df = pd.DataFrame({"id": ids})
    result = check_duplicates(df, "ds", "id")
    assert result["status"] == expected_status
    assert result["error_count"] == expected_count


def test_check_duplicates_missing_column_raises() -> None:
    df = pd.DataFrame({"a": [1, 2]})
    with pytest.raises(ValueError, match="Missing required columns"):
        check_duplicates(df, "ds", "nonexistent")


# ─────────────────────────────────────────────────────────────────────────────
# check_schema
# ─────────────────────────────────────────────────────────────────────────────

def test_check_schema_passes_when_all_cols_present(customers_df: pd.DataFrame) -> None:
    result = check_schema(customers_df, "customers", ["customer_id", "city", "state"])
    assert result["status"] == "PASS"


def test_check_schema_fails_when_col_missing(customers_df: pd.DataFrame) -> None:
    result = check_schema(customers_df, "customers", ["customer_id", "nonexistent_column"])
    assert result["status"] == "FAIL"
    assert "nonexistent_column" in result["details"]["missing_columns"]


# ─────────────────────────────────────────────────────────────────────────────
# clean_customers
# ─────────────────────────────────────────────────────────────────────────────

class TestCleanCustomers:
    def _raw(self) -> pd.DataFrame:
        return pd.DataFrame({
            "customer_id": [1, 2, 3],
            "name":        ["Alice", "Bob", "Carol"],
            "email":       ["a@x.com", "b@x.com", "c@x.com"],
            "phone":       ["11999", "11888", "11777"],
            "city":        ["SP", "RJ", "MG"],
            "state":       ["SP", "RJ", "MG"],
            "birth_date":  ["1990-01-01", "1985-01-01", "2000-01-01"],
            "created_at":  ["2020-01-01", "2021-01-01", "2022-01-01"],
        })

    def test_pii_columns_are_hashed(self) -> None:
        result = clean_customers(self._raw())
        assert "Alice"   not in result["name"].values
        assert "a@x.com" not in result["email"].values
        assert 1         not in result["customer_id"].values

    def test_all_hashes_are_64_char_lowercase_hex(self) -> None:
        result = clean_customers(self._raw())
        for col in ("customer_id", "name", "email"):
            assert result[col].str.len().eq(64).all(), f"{col} hashes not 64 chars"
            assert result[col].str.match(r"^[0-9a-f]{64}$").all(), f"{col} not hex"

    def test_customer_id_hash_matches_anonymise(self) -> None:
        """The hash produced by clean_customers must equal _anonymise() directly."""
        raw = self._raw()
        result = clean_customers(raw)
        expected = _anonymise(1)
        assert result.loc[0, "customer_id"] == expected

    def test_drops_rows_with_null_customer_id(self) -> None:
        df = self._raw()
        df.loc[0, "customer_id"] = None
        result = clean_customers(df)
        assert len(result) == 2

    def test_drops_exact_duplicates(self) -> None:
        df = pd.concat([self._raw(), self._raw()], ignore_index=True)
        result = clean_customers(df)
        assert len(result) == 3

    def test_created_at_is_datetime(self) -> None:
        result = clean_customers(self._raw())
        assert pd.api.types.is_datetime64_any_dtype(result["created_at"])

    def test_hashes_are_consistent_across_calls(self) -> None:
        """Same input → same output across two separate clean_customers() calls."""
        r1 = clean_customers(self._raw())
        r2 = clean_customers(self._raw())
        assert (r1["customer_id"] == r2["customer_id"]).all()

    def test_customer_id_hash_in_orders_matches_customers(self) -> None:
        """FK consistency: _anonymise(1) in orders == _anonymise(1) in customers."""
        raw_customers = self._raw()
        raw_orders = pd.DataFrame({
            "order_id":     [101],
            "customer_id":  [1],  # same raw value as customer_id=1 in customers
            "order_date":   ["2023-01-10"],
            "status":       ["delivered"],
            "total_amount": [100.0],
            "shipping_fee": [10.0],
            "channel":      ["web"],
        })
        cleaned_customers = clean_customers(raw_customers)
        cleaned_orders    = clean_orders(raw_orders)

        # FK in orders must appear in customers PK set
        order_cids    = set(cleaned_orders["customer_id"])
        customer_cids = set(cleaned_customers["customer_id"])
        assert order_cids <= customer_cids, "Hashed customer_id mismatch between orders and customers"


# ─────────────────────────────────────────────────────────────────────────────
# clean_products
# ─────────────────────────────────────────────────────────────────────────────

class TestCleanProducts:
    def _raw(self) -> pd.DataFrame:
        return pd.DataFrame({
            "product_id":  [1, 2, 3],
            "name":        ["A", "B", "C"],
            "category":    ["X", "Y", "Z"],
            "subcategory": ["x", "y", "z"],
            "price":       [100.0, 50.0, 25.0],
            "cost":        [40.0, 20.0, 10.0],
            "stock_qty":   [10, None, 5],
            "created_at":  ["2021-01-01", "2021-01-01", "2021-01-01"],
        })

    def test_null_stock_qty_filled_with_zero(self) -> None:
        result = clean_products(self._raw())
        assert result.loc[result["product_id"] == 2, "stock_qty"].iloc[0] == 0

    def test_duplicate_product_ids_removed(self) -> None:
        df = pd.concat([self._raw(), self._raw()], ignore_index=True)
        result = clean_products(df)
        assert len(result) == 3

    def test_cost_exceeding_price_is_capped(self) -> None:
        df = pd.DataFrame({
            "product_id": [1], "name": ["X"], "category": ["C"], "subcategory": ["s"],
            "price": [10.0], "cost": [15.0],  # cost > price → invalid
            "stock_qty": [1], "created_at": ["2021-01-01"],
        })
        result = clean_products(df)
        assert float(result.loc[0, "cost"]) <= float(result.loc[0, "price"])


# ─────────────────────────────────────────────────────────────────────────────
# clean_order_items
# ─────────────────────────────────────────────────────────────────────────────

@pytest.mark.parametrize("discount,expected", [
    (-0.10, 0.0),
    (0.50,  0.5),
    (1.50,  1.0),
    (0.0,   0.0),
    (1.0,   1.0),
])
def test_clean_order_items_discount_clipping(discount: float, expected: float) -> None:
    df = pd.DataFrame({
        "item_id":    [1], "order_id": [10], "product_id": [1],
        "quantity":   [1], "unit_price": [50.0],
        "discount":   [discount], "line_total": [50.0],
    })
    result = clean_order_items(df)
    assert abs(float(result.loc[0, "discount"]) - expected) < 1e-9


# ─────────────────────────────────────────────────────────────────────────────
# clean_orders — FK hash consistency (critical regression test)
# ─────────────────────────────────────────────────────────────────────────────

def test_clean_orders_customer_id_hash_consistent_with_customers() -> None:
    """Regression: orders and customers must hash the same raw customer_id identically."""
    raw_cid = 42
    from pipeline.transformation.transform import clean_customers, clean_orders

    raw_customers = pd.DataFrame({
        "customer_id": [raw_cid], "name": ["X"], "email": ["x@x.com"], "phone": ["1"],
        "city": ["A"], "state": ["SP"], "birth_date": ["1990-01-01"], "created_at": ["2020-01-01"],
    })
    raw_orders = pd.DataFrame({
        "order_id": [1], "customer_id": [raw_cid], "order_date": ["2023-01-01"],
        "status": ["delivered"], "total_amount": [99.0], "shipping_fee": [9.0], "channel": ["web"],
    })

    c_hash = clean_customers(raw_customers).loc[0, "customer_id"]
    o_hash = clean_orders(raw_orders).loc[0, "customer_id"]

    assert c_hash == o_hash, (
        "CRITICAL: customer_id hash differs between customers and orders tables. "
        "All cross-table joins after LGPD anonymisation will silently fail."
    )
