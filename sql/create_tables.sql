-- =============================================================================
-- create_tables.sql
-- DDL for the e-commerce data warehouse (PostgreSQL-compatible syntax)
-- =============================================================================

-- ─────────────────────────────────────────────────────────────────────────────
-- Schema
-- ─────────────────────────────────────────────────────────────────────────────
CREATE SCHEMA IF NOT EXISTS dw;

-- ─────────────────────────────────────────────────────────────────────────────
-- Dimension: dim_clientes
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS dw.dim_clientes (
    sk_cliente       SERIAL          PRIMARY KEY,        -- surrogate key
    nk_customer_id   VARCHAR(64)     NOT NULL UNIQUE,    -- SHA-256 natural key (LGPD)
    city             VARCHAR(100),
    state            CHAR(2),
    dt_criacao       TIMESTAMP,
    dt_carga         TIMESTAMP       DEFAULT NOW()       -- ETL load timestamp
);

COMMENT ON TABLE  dw.dim_clientes              IS 'Customer dimension – PII anonymised per LGPD.';
COMMENT ON COLUMN dw.dim_clientes.nk_customer_id IS 'HMAC-SHA256 (keyed hash) of the original customer identifier — plain SHA-256 is not used because it is vulnerable to rainbow-table attacks.';

CREATE INDEX IF NOT EXISTS idx_dim_clientes_state ON dw.dim_clientes (state);

-- ─────────────────────────────────────────────────────────────────────────────
-- Dimension: dim_produtos
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS dw.dim_produtos (
    sk_produto       SERIAL          PRIMARY KEY,
    nk_product_id    INT             NOT NULL UNIQUE,
    nm_produto       VARCHAR(255)    NOT NULL,
    category         VARCHAR(100),
    subcategory      VARCHAR(100),
    price            NUMERIC(10, 2)  CHECK (price >= 0),
    cost             NUMERIC(10, 2)  CHECK (cost  >= 0),
    margem_pct       NUMERIC(6, 2),
    dt_carga         TIMESTAMP       DEFAULT NOW()
);

COMMENT ON TABLE dw.dim_produtos IS 'Product dimension including category hierarchy and margin.';

CREATE INDEX IF NOT EXISTS idx_dim_produtos_category ON dw.dim_produtos (category);

-- ─────────────────────────────────────────────────────────────────────────────
-- Dimension: dim_tempo
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS dw.dim_tempo (
    sk_tempo         SERIAL          PRIMARY KEY,
    dt_data          DATE            NOT NULL UNIQUE,
    ano              SMALLINT        NOT NULL,
    mes              SMALLINT        NOT NULL CHECK (mes BETWEEN 1 AND 12),
    dia              SMALLINT        NOT NULL CHECK (dia BETWEEN 1 AND 31),
    trimestre        SMALLINT        NOT NULL CHECK (trimestre BETWEEN 1 AND 4),
    semana_ano       SMALLINT,
    dia_semana       SMALLINT        CHECK (dia_semana BETWEEN 0 AND 6),
    nm_dia_semana    VARCHAR(15),
    nm_mes           VARCHAR(15),
    fl_fimdesemana   BOOLEAN         DEFAULT FALSE
);

COMMENT ON TABLE dw.dim_tempo IS 'Date dimension spanning the full order history range.';

CREATE INDEX IF NOT EXISTS idx_dim_tempo_ano_mes ON dw.dim_tempo (ano, mes);

-- ─────────────────────────────────────────────────────────────────────────────
-- Fact: fato_pedidos
-- Grain: one row per order-item line
-- Partitioned by year, month (declaration only – apply at DDL level in PG 13+)
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS dw.fato_pedidos (
    nk_order_id       VARCHAR(50)     NOT NULL,
    item_id           INT,
    sk_cliente        INT             REFERENCES dw.dim_clientes (sk_cliente),
    sk_produto        INT             REFERENCES dw.dim_produtos  (sk_produto),
    sk_tempo          INT             REFERENCES dw.dim_tempo     (sk_tempo),
    ds_status         VARCHAR(30),
    ds_canal          VARCHAR(30),
    payment_status    VARCHAR(30),
    method            VARCHAR(30),
    quantity          SMALLINT        CHECK (quantity > 0),
    unit_price        NUMERIC(10, 2)  CHECK (unit_price >= 0),
    discount          NUMERIC(5, 4)   CHECK (discount BETWEEN 0 AND 1),
    line_total        NUMERIC(12, 2)  CHECK (line_total >= 0),
    vl_total_pedido   NUMERIC(12, 2),
    vl_frete          NUMERIC(8, 2),
    year              SMALLINT,
    month             SMALLINT,
    dt_carga          TIMESTAMP       DEFAULT NOW()
);

COMMENT ON TABLE dw.fato_pedidos IS 'Order fact – grain: one row per order-item. Partitioned by year/month.';

CREATE INDEX IF NOT EXISTS idx_fato_pedidos_cliente  ON dw.fato_pedidos (sk_cliente);
CREATE INDEX IF NOT EXISTS idx_fato_pedidos_produto  ON dw.fato_pedidos (sk_produto);
CREATE INDEX IF NOT EXISTS idx_fato_pedidos_tempo    ON dw.fato_pedidos (sk_tempo);
CREATE INDEX IF NOT EXISTS idx_fato_pedidos_status   ON dw.fato_pedidos (ds_status);
CREATE INDEX IF NOT EXISTS idx_fato_pedidos_ym       ON dw.fato_pedidos (year, month);
