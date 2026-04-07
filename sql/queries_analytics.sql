-- =============================================================================
-- queries_analytics.sql
-- Queries analíticas sobre o esquema estrela do data warehouse
-- =============================================================================

-- ─────────────────────────────────────────────────────────────────────────────
-- Q1: Receita mensal por categoria (somente pedidos entregues)
-- Retorna: ano, mês, categoria, receita, quantidade de pedidos, ticket médio
-- ─────────────────────────────────────────────────────────────────────────────
SELECT
    t.ano                                           AS ano,
    t.mes                                           AS mes,
    p.category                                      AS categoria,
    SUM(f.line_total)                               AS receita,
    COUNT(DISTINCT f.nk_order_id)                   AS qtd_pedidos,
    ROUND(SUM(f.line_total) / NULLIF(COUNT(DISTINCT f.nk_order_id), 0), 2)
                                                    AS ticket_medio
FROM       dw.fato_pedidos   f
JOIN       dw.dim_tempo      t ON t.sk_tempo  = f.sk_tempo
JOIN       dw.dim_produtos   p ON p.sk_produto = f.sk_produto
WHERE      f.ds_status = 'delivered'
GROUP BY   t.ano, t.mes, p.category
ORDER BY   t.ano, t.mes, receita DESC;


-- ─────────────────────────────────────────────────────────────────────────────
-- Q2: Top 20 produtos mais vendidos (por receita, todo o período)
-- ─────────────────────────────────────────────────────────────────────────────
SELECT
    p.nk_product_id,
    p.nm_produto,
    p.category                                      AS categoria,
    SUM(f.quantity)                                 AS unidades_vendidas,
    SUM(f.line_total)                               AS receita_total,
    RANK() OVER (ORDER BY SUM(f.line_total) DESC)   AS ranking_receita
FROM       dw.fato_pedidos  f
JOIN       dw.dim_produtos  p ON p.sk_produto = f.sk_produto
WHERE      f.ds_status NOT IN ('cancelled', 'returned')
GROUP BY   p.nk_product_id, p.nm_produto, p.category
ORDER BY   receita_total DESC
LIMIT      20;


-- ─────────────────────────────────────────────────────────────────────────────
-- Q3: Ticket médio por canal e método de pagamento (últimos 12 meses)
-- ─────────────────────────────────────────────────────────────────────────────
SELECT
    f.ds_canal                                          AS canal,
    f.method                                            AS metodo_pagamento,
    COUNT(DISTINCT f.nk_order_id)                       AS qtd_pedidos,
    ROUND(AVG(f.vl_total_pedido), 2)                    AS ticket_medio,
    ROUND(SUM(f.line_total), 2)                         AS receita_total
FROM       dw.fato_pedidos  f
JOIN       dw.dim_tempo     t ON t.sk_tempo = f.sk_tempo
WHERE      t.dt_data >= CURRENT_DATE - INTERVAL '12 months'
  AND      f.ds_status NOT IN ('cancelled', 'returned')
GROUP BY   f.ds_canal, f.method
ORDER BY   ticket_medio DESC;


-- ─────────────────────────────────────────────────────────────────────────────
-- Q4: Indicador de churn de clientes
-- "Churn" = realizou ao menos um pedido mas nenhum nos últimos 90 dias
-- ─────────────────────────────────────────────────────────────────────────────
WITH ultimo_pedido_cliente AS (
    SELECT
        f.sk_cliente,
        MAX(t.dt_data) AS dt_ultimo_pedido
    FROM   dw.fato_pedidos  f
    JOIN   dw.dim_tempo     t ON t.sk_tempo = f.sk_tempo
    WHERE  f.ds_status NOT IN ('cancelled', 'returned')
    GROUP BY f.sk_cliente
)
SELECT
    c.sk_cliente,
    c.state                                             AS estado,
    upc.dt_ultimo_pedido,
    CURRENT_DATE - upc.dt_ultimo_pedido                 AS dias_desde_ultimo_pedido,
    CASE
        WHEN CURRENT_DATE - upc.dt_ultimo_pedido > 90 THEN 'churn'
        ELSE 'ativo'
    END                                                 AS status_churn
FROM       ultimo_pedido_cliente  upc
JOIN       dw.dim_clientes        c ON c.sk_cliente = upc.sk_cliente
ORDER BY   dias_desde_ultimo_pedido DESC;


-- ─────────────────────────────────────────────────────────────────────────────
-- Q5: Análise de coorte por primeira compra
-- Agrupa clientes pelo mês da primeira compra e acompanha
-- quantos continuam comprando nos meses seguintes.
-- ─────────────────────────────────────────────────────────────────────────────
WITH primeiros_pedidos AS (
    SELECT
        f.sk_cliente,
        MIN(t.dt_data)                                     AS dt_primeiro_pedido,
        DATE_TRUNC('month', MIN(t.dt_data))                AS mes_coorte
    FROM   dw.fato_pedidos  f
    JOIN   dw.dim_tempo     t ON t.sk_tempo = f.sk_tempo
    WHERE  f.ds_status NOT IN ('cancelled', 'returned')
    GROUP BY f.sk_cliente
),
pedidos_subsequentes AS (
    SELECT
        pp.mes_coorte,
        DATE_TRUNC('month', t.dt_data)                     AS mes_pedido,
        COUNT(DISTINCT f.sk_cliente)                        AS clientes_ativos
    FROM   dw.fato_pedidos  f
    JOIN   dw.dim_tempo     t  ON t.sk_tempo   = f.sk_tempo
    JOIN   primeiros_pedidos pp ON pp.sk_cliente = f.sk_cliente
    WHERE  f.ds_status NOT IN ('cancelled', 'returned')
    GROUP BY pp.mes_coorte, DATE_TRUNC('month', t.dt_data)
)
SELECT
    mes_coorte,
    mes_pedido,
    clientes_ativos,
    EXTRACT(MONTH FROM AGE(mes_pedido, mes_coorte))         AS numero_mes
FROM   pedidos_subsequentes
ORDER BY mes_coorte, mes_pedido;
