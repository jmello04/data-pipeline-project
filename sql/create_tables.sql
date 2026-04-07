-- =============================================================================
-- create_tables.sql
-- DDL do data warehouse de e-commerce (sintaxe compatível com PostgreSQL)
-- =============================================================================

-- ─────────────────────────────────────────────────────────────────────────────
-- Schema
-- ─────────────────────────────────────────────────────────────────────────────
CREATE SCHEMA IF NOT EXISTS dw;

-- ─────────────────────────────────────────────────────────────────────────────
-- Dimensão: dim_clientes
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS dw.dim_clientes (
    sk_cliente       SERIAL          PRIMARY KEY,        -- chave substituta
    nk_customer_id   VARCHAR(64)     NOT NULL UNIQUE,    -- chave natural com hash SHA-256 (LGPD)
    city             VARCHAR(100),
    state            CHAR(2),
    dt_criacao       TIMESTAMP,
    dt_carga         TIMESTAMP       DEFAULT NOW()       -- timestamp da carga ETL
);

COMMENT ON TABLE  dw.dim_clientes                IS 'Dimensão de clientes – PII anonimizado conforme LGPD.';
COMMENT ON COLUMN dw.dim_clientes.nk_customer_id IS 'HMAC-SHA256 (hash com chave) do identificador original do cliente — SHA-256 simples não é usado por ser vulnerável a ataques de rainbow table.';

CREATE INDEX IF NOT EXISTS idx_dim_clientes_state ON dw.dim_clientes (state);

-- ─────────────────────────────────────────────────────────────────────────────
-- Dimensão: dim_produtos
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

COMMENT ON TABLE dw.dim_produtos IS 'Dimensão de produtos com hierarquia de categoria e margem calculada.';

CREATE INDEX IF NOT EXISTS idx_dim_produtos_category ON dw.dim_produtos (category);

-- ─────────────────────────────────────────────────────────────────────────────
-- Dimensão: dim_tempo
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

COMMENT ON TABLE dw.dim_tempo IS 'Dimensão de datas cobrindo todo o histórico de pedidos.';

CREATE INDEX IF NOT EXISTS idx_dim_tempo_ano_mes ON dw.dim_tempo (ano, mes);

-- ─────────────────────────────────────────────────────────────────────────────
-- Fato: fato_pedidos
-- Grão: uma linha por item de pedido
-- Particionado por ano e mês (declaração apenas – aplicar no DDL do PG 13+)
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

COMMENT ON TABLE dw.fato_pedidos IS 'Fato de pedidos – grão: uma linha por item de pedido. Particionado por ano/mês.';

CREATE INDEX IF NOT EXISTS idx_fato_pedidos_cliente  ON dw.fato_pedidos (sk_cliente);
CREATE INDEX IF NOT EXISTS idx_fato_pedidos_produto  ON dw.fato_pedidos (sk_produto);
CREATE INDEX IF NOT EXISTS idx_fato_pedidos_tempo    ON dw.fato_pedidos (sk_tempo);
CREATE INDEX IF NOT EXISTS idx_fato_pedidos_status   ON dw.fato_pedidos (ds_status);
CREATE INDEX IF NOT EXISTS idx_fato_pedidos_ym       ON dw.fato_pedidos (year, month);
