"""
tests/test_warehouse.py

Unit tests for the Gold-layer dimensional modelling functions.

Coverage:
    - _crc32_sk: determinism, positivity, collision detection, type handling
    - build_dim_clientes: schema, SK derivation
    - build_dim_produtos: schema, safe margin calculation (price = 0 edge case)
    - build_dim_tempo: date range, attribute correctness, edge cases
    - build_fato_pedidos: year/month correctness for multi-item orders (regression
      for the slicing bug where orders["year"].values[:len(fato)] produced
      wrong results when one order had multiple items)
"""

import os
from datetime import date

import pandas as pd
import pytest
from hypothesis import given, settings as h_settings
from hypothesis import strategies as st

os.environ["LGPD_HASH_KEY"] = "test-secret-key-for-unit-tests-only-abcdef"

from pipeline.warehouse.dw_model import (
    _crc32_sk,
    _build_sk_column,
    build_dim_clientes,
    build_dim_produtos,
    build_dim_tempo,
    build_fato_pedidos,
)
from pipeline.transformation.transform import _anonymise


# ─────────────────────────────────────────────────────────────────────────────
# Fixtures
# ─────────────────────────────────────────────────────────────────────────────

@pytest.fixture(scope="module")
def customers_silver() -> pd.DataFrame:
    """Minimal Silver-layer customers DataFrame (HMAC-anonymised)."""
    return pd.DataFrame({
        "customer_id": [_anonymise(1), _anonymise(2), _anonymise(3)],
        "city":        ["São Paulo", "Rio de Janeiro", "Belo Horizonte"],
        "state":       ["SP", "RJ", "MG"],
        "created_at":  pd.to_datetime(["2020-01-01", "2021-03-10", "2022-07-05"]),
    })


@pytest.fixture(scope="module")
def products_silver() -> pd.DataFrame:
    """Minimal Silver-layer products DataFrame."""
    return pd.DataFrame({
        "product_id":  [10, 20, 30],
        "name":        ["Widget A", "Gadget B", "Tool C"],
        "category":    ["Electronics", "Electronics", "Home"],
        "subcategory": ["Accessories", "Laptops", "Kitchen"],
        "price":       [100.00, 500.00, 50.00],
        "cost":        [40.00,  200.00, 20.00],
        "stock_qty":   [100, 50, 200],
    })


@pytest.fixture(scope="module")
def orders_silver() -> pd.DataFrame:
    """3 orders: order 1 has 2 items (tests multi-item year/month correctness)."""
    return pd.DataFrame({
        "order_id":     [1, 2, 3],
        "customer_id":  [_anonymise(1), _anonymise(2), _anonymise(3)],
        "order_date":   pd.to_datetime(["2022-03-15", "2023-06-20", "2023-12-01"]),
        "status":       ["delivered", "shipped", "cancelled"],
        "total_amount": [200.00, 500.00, 50.00],
        "shipping_fee": [15.00, 0.00, 10.00],
        "channel":      ["web", "mobile", "web"],
    })


@pytest.fixture(scope="module")
def items_silver() -> pd.DataFrame:
    """Order 1 intentionally has 2 items to exercise the multi-item bug."""
    return pd.DataFrame({
        "item_id":    [1, 2, 3, 4],
        "order_id":   [1, 1, 2, 3],        # order 1 → 2 items
        "product_id": [10, 20, 10, 30],
        "quantity":   [2,  1,  3,  1],
        "unit_price": [100.00, 500.00, 100.00, 50.00],
        "discount":   [0.0,    0.0,    0.0,    0.0],
        "line_total": [200.00, 500.00, 300.00, 50.00],
    })


@pytest.fixture(scope="module")
def payments_silver() -> pd.DataFrame:
    return pd.DataFrame({
        "payment_id": [1, 2, 3],
        "order_id":   [1, 2, 3],
        "method":     ["pix", "credit_card", "boleto"],
        "status":     ["approved", "approved", "declined"],
        "amount":     [200.00, 500.00, 50.00],
    })


@pytest.fixture(scope="module")
def dim_clientes(customers_silver: pd.DataFrame) -> pd.DataFrame:
    return build_dim_clientes(customers_silver)


@pytest.fixture(scope="module")
def dim_produtos(products_silver: pd.DataFrame) -> pd.DataFrame:
    return build_dim_produtos(products_silver)


@pytest.fixture(scope="module")
def dim_tempo() -> pd.DataFrame:
    return build_dim_tempo(date(2022, 1, 1), date(2023, 12, 31))


@pytest.fixture(scope="module")
def fato(orders_silver, items_silver, payments_silver,
         dim_clientes, dim_produtos, dim_tempo) -> pd.DataFrame:
    return build_fato_pedidos(
        orders_silver, items_silver, payments_silver,
        dim_clientes, dim_produtos, dim_tempo,
    )


# ─────────────────────────────────────────────────────────────────────────────
# _crc32_sk
# ─────────────────────────────────────────────────────────────────────────────

class TestCrc32Sk:
    def test_returns_positive_integer(self) -> None:
        assert _crc32_sk("test") > 0

    def test_deterministic(self) -> None:
        assert _crc32_sk("hello") == _crc32_sk("hello")

    def test_different_inputs_produce_different_values(self) -> None:
        assert _crc32_sk("a") != _crc32_sk("b")

    def test_integer_and_string_of_same_value_differ(self) -> None:
        # Natural key 1 (int) vs "1" (str) — CRC32 of "1" vs "1" same,
        # but int will be str(1)="1" so they should be equal.
        assert _crc32_sk(1) == _crc32_sk("1")

    def test_never_returns_zero(self) -> None:
        # 0 is the unknown-member sentinel; _crc32_sk must never produce it.
        # We can't exhaustively test all inputs, but verify common values.
        for v in range(1, 10_000):
            assert _crc32_sk(v) != 0

    @given(st.text(min_size=1, max_size=100))
    @h_settings(max_examples=200)
    def test_property_always_positive_nonzero(self, value: str) -> None:
        result = _crc32_sk(value)
        assert result > 0
        assert result != 0

    @given(st.integers(min_value=1, max_value=10_000_000))
    @h_settings(max_examples=200)
    def test_property_integer_keys_always_positive(self, value: int) -> None:
        assert _crc32_sk(value) > 0


# ─────────────────────────────────────────────────────────────────────────────
# build_dim_clientes
# ─────────────────────────────────────────────────────────────────────────────

class TestBuildDimClientes:
    def test_output_has_correct_columns(self, dim_clientes: pd.DataFrame) -> None:
        assert set(dim_clientes.columns) == {"sk_cliente", "nk_customer_id", "city", "state", "dt_criacao"}

    def test_row_count_matches_input(self, customers_silver: pd.DataFrame, dim_clientes: pd.DataFrame) -> None:
        assert len(dim_clientes) == len(customers_silver)

    def test_sk_cliente_is_positive_integer(self, dim_clientes: pd.DataFrame) -> None:
        assert (dim_clientes["sk_cliente"] > 0).all()

    def test_sk_cliente_is_unique(self, dim_clientes: pd.DataFrame) -> None:
        assert dim_clientes["sk_cliente"].nunique() == len(dim_clientes)

    def test_sk_cliente_is_deterministic(self, customers_silver: pd.DataFrame) -> None:
        """Two builds from the same input must produce identical SKs."""
        d1 = build_dim_clientes(customers_silver)
        d2 = build_dim_clientes(customers_silver)
        assert (d1["sk_cliente"] == d2["sk_cliente"]).all()

    def test_nk_customer_id_is_hmac_hash(self, dim_clientes: pd.DataFrame) -> None:
        assert dim_clientes["nk_customer_id"].str.len().eq(64).all()


# ─────────────────────────────────────────────────────────────────────────────
# build_dim_produtos
# ─────────────────────────────────────────────────────────────────────────────

class TestBuildDimProdutos:
    def test_output_has_margem_pct_column(self, dim_produtos: pd.DataFrame) -> None:
        assert "margem_pct" in dim_produtos.columns

    def test_margin_is_positive_when_cost_less_than_price(self, dim_produtos: pd.DataFrame) -> None:
        assert (dim_produtos["margem_pct"] > 0).all()

    def test_margin_formula_is_correct(self, products_silver: pd.DataFrame) -> None:
        dim = build_dim_produtos(products_silver)
        row = dim[dim["nk_product_id"] == 10].iloc[0]
        expected = round((100.00 - 40.00) / 100.00 * 100, 2)
        assert abs(float(row["margem_pct"]) - expected) < 0.01

    def test_zero_price_does_not_raise(self) -> None:
        """price = 0 must produce margem_pct = 0.0, not ZeroDivisionError."""
        df = pd.DataFrame({
            "product_id": [99], "name": ["Free"], "category": ["X"],
            "subcategory": ["y"], "price": [0.0], "cost": [0.0], "stock_qty": [0],
        })
        dim = build_dim_produtos(df)
        assert float(dim.loc[0, "margem_pct"]) == 0.0

    def test_sk_produto_is_deterministic(self, products_silver: pd.DataFrame) -> None:
        d1 = build_dim_produtos(products_silver)
        d2 = build_dim_produtos(products_silver)
        assert (d1["sk_produto"] == d2["sk_produto"]).all()


# ─────────────────────────────────────────────────────────────────────────────
# build_dim_tempo
# ─────────────────────────────────────────────────────────────────────────────

class TestBuildDimTempo:
    def test_row_count_matches_date_range(self, dim_tempo: pd.DataFrame) -> None:
        # 2022-01-01 to 2023-12-31 = 730 days (2022 is not a leap year but 2023 is not either —
        # 365 + 365 = 730)
        assert len(dim_tempo) == 730

    def test_all_required_columns_present(self, dim_tempo: pd.DataFrame) -> None:
        required = {"sk_tempo", "dt_data", "ano", "mes", "dia", "trimestre",
                    "semana_ano", "dia_semana", "nm_dia_semana", "nm_mes", "fl_fimdesemana"}
        assert required <= set(dim_tempo.columns)

    def test_month_range(self, dim_tempo: pd.DataFrame) -> None:
        assert dim_tempo["mes"].between(1, 12).all()

    def test_day_range(self, dim_tempo: pd.DataFrame) -> None:
        assert dim_tempo["dia"].between(1, 31).all()

    def test_quarter_range(self, dim_tempo: pd.DataFrame) -> None:
        assert dim_tempo["trimestre"].between(1, 4).all()

    def test_weekday_flags_are_correct(self, dim_tempo: pd.DataFrame) -> None:
        weekends = dim_tempo[dim_tempo["fl_fimdesemana"]]
        assert (weekends["dia_semana"] >= 5).all()

    def test_sk_tempo_is_unique(self, dim_tempo: pd.DataFrame) -> None:
        assert dim_tempo["sk_tempo"].nunique() == len(dim_tempo)

    def test_raises_when_start_after_end(self) -> None:
        with pytest.raises(ValueError, match="must be ≤"):
            build_dim_tempo(date(2025, 1, 1), date(2024, 1, 1))

    def test_single_day_range(self) -> None:
        dim = build_dim_tempo(date(2024, 6, 15), date(2024, 6, 15))
        assert len(dim) == 1
        assert int(dim.iloc[0]["ano"]) == 2024
        assert int(dim.iloc[0]["mes"]) == 6
        assert int(dim.iloc[0]["dia"]) == 15


# ─────────────────────────────────────────────────────────────────────────────
# build_fato_pedidos
# ─────────────────────────────────────────────────────────────────────────────

class TestBuildFatoPedidos:
    def test_row_count_equals_total_items(self, fato: pd.DataFrame) -> None:
        """Grain is order × item — 4 items → 4 rows."""
        assert len(fato) == 4

    def test_required_columns_present(self, fato: pd.DataFrame) -> None:
        required = {"nk_order_id", "item_id", "sk_cliente", "sk_produto", "sk_tempo",
                    "ds_status", "ds_canal", "year", "month"}
        assert required <= set(fato.columns)

    def test_year_and_month_correct_for_multi_item_order(self, fato: pd.DataFrame) -> None:
        """
        Regression test for the year/month slicing bug.

        Order 1 has 2 items.  Both item rows must carry year=2022, month=3
        (from order_date 2022-03-15).  The old bug — slicing
        orders["year"].values[:len(fato)] — would have assigned wrong
        year/month to the second item row of order 1.
        """
        order1_rows = fato[fato["nk_order_id"] == 1]
        assert len(order1_rows) == 2, "Order 1 must produce 2 fact rows (2 items)"

        years  = order1_rows["year"].unique()
        months = order1_rows["month"].unique()

        assert list(years)  == [2022], f"Both rows must have year=2022, got {years}"
        assert list(months) == [3],    f"Both rows must have month=3,  got {months}"

    def test_year_correct_for_single_item_orders(self, fato: pd.DataFrame) -> None:
        row2 = fato[fato["nk_order_id"] == 2].iloc[0]
        assert int(row2["year"])  == 2023
        assert int(row2["month"]) == 6

        row3 = fato[fato["nk_order_id"] == 3].iloc[0]
        assert int(row3["year"])  == 2023
        assert int(row3["month"]) == 12

    def test_sk_cliente_is_nonzero_for_known_customers(self, fato: pd.DataFrame) -> None:
        """All orders reference known customers → no unknown-member SK (0)."""
        assert (fato["sk_cliente"] != 0).all()

    def test_sk_produto_is_nonzero_for_known_products(self, fato: pd.DataFrame) -> None:
        assert (fato["sk_produto"] != 0).all()

    def test_sk_tempo_is_nonzero_for_dates_in_dimension(self, fato: pd.DataFrame) -> None:
        assert (fato["sk_tempo"] != 0).all()

    def test_surrogate_keys_are_deterministic(
        self,
        orders_silver, items_silver, payments_silver,
        dim_clientes, dim_produtos, dim_tempo,
    ) -> None:
        """Running build_fato_pedidos twice produces identical SKs."""
        f1 = build_fato_pedidos(orders_silver, items_silver, payments_silver,
                                dim_clientes, dim_produtos, dim_tempo)
        f2 = build_fato_pedidos(orders_silver, items_silver, payments_silver,
                                dim_clientes, dim_produtos, dim_tempo)
        assert (f1["sk_cliente"] == f2["sk_cliente"]).all()
        assert (f1["sk_produto"] == f2["sk_produto"]).all()
        assert (f1["sk_tempo"]   == f2["sk_tempo"]).all()

    def test_orphan_customer_sk_is_zero(
        self,
        orders_silver, items_silver, payments_silver,
        dim_produtos, dim_tempo,
    ) -> None:
        """Orders with customer_id not in dim_clientes receive SK = 0."""
        orders_orphan = orders_silver.copy()
        orders_orphan["customer_id"] = "nonexistent-hash"

        empty_dim_clientes = pd.DataFrame(
            columns=["sk_cliente", "nk_customer_id", "city", "state", "dt_criacao"]
        )
        fato_orphan = build_fato_pedidos(
            orders_orphan, items_silver, payments_silver,
            empty_dim_clientes, dim_produtos, dim_tempo,
        )
        assert (fato_orphan["sk_cliente"] == 0).all()
