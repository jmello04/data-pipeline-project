-- =============================================================================
-- queries_analytics.sql
-- Analytical queries against the data warehouse star schema
-- =============================================================================

-- ─────────────────────────────────────────────────────────────────────────────
-- Q1: Monthly revenue by category (delivered orders only)
-- Returns: year, month, category, revenue, order_count, avg_ticket
-- ─────────────────────────────────────────────────────────────────────────────
SELECT
    t.ano                                           AS year,
    t.mes                                           AS month,
    p.category,
    SUM(f.line_total)                               AS revenue,
    COUNT(DISTINCT f.nk_order_id)                   AS order_count,
    ROUND(SUM(f.line_total) / NULLIF(COUNT(DISTINCT f.nk_order_id), 0), 2)
                                                    AS avg_ticket
FROM       dw.fato_pedidos   f
JOIN       dw.dim_tempo      t ON t.sk_tempo  = f.sk_tempo
JOIN       dw.dim_produtos   p ON p.sk_produto = f.sk_produto
WHERE      f.ds_status = 'delivered'
GROUP BY   t.ano, t.mes, p.category
ORDER BY   t.ano, t.mes, revenue DESC;


-- ─────────────────────────────────────────────────────────────────────────────
-- Q2: Top 20 best-selling products (by revenue, all time)
-- ─────────────────────────────────────────────────────────────────────────────
SELECT
    p.nk_product_id,
    p.nm_produto,
    p.category,
    SUM(f.quantity)    AS units_sold,
    SUM(f.line_total)  AS total_revenue,
    RANK() OVER (ORDER BY SUM(f.line_total) DESC) AS revenue_rank
FROM       dw.fato_pedidos  f
JOIN       dw.dim_produtos  p ON p.sk_produto = f.sk_produto
WHERE      f.ds_status NOT IN ('cancelled', 'returned')
GROUP BY   p.nk_product_id, p.nm_produto, p.category
ORDER BY   total_revenue DESC
LIMIT      20;


-- ─────────────────────────────────────────────────────────────────────────────
-- Q3: Average ticket per channel and payment method (last 12 months)
-- ─────────────────────────────────────────────────────────────────────────────
SELECT
    f.ds_canal                                          AS channel,
    f.method                                            AS payment_method,
    COUNT(DISTINCT f.nk_order_id)                       AS order_count,
    ROUND(AVG(f.vl_total_pedido), 2)                    AS avg_ticket,
    ROUND(SUM(f.line_total), 2)                         AS total_revenue
FROM       dw.fato_pedidos  f
JOIN       dw.dim_tempo     t ON t.sk_tempo = f.sk_tempo
WHERE      t.dt_data >= CURRENT_DATE - INTERVAL '12 months'
  AND      f.ds_status NOT IN ('cancelled', 'returned')
GROUP BY   f.ds_canal, f.method
ORDER BY   avg_ticket DESC;


-- ─────────────────────────────────────────────────────────────────────────────
-- Q4: Customer churn indicator
-- "Churned" = placed at least one order but none in the last 90 days
-- ─────────────────────────────────────────────────────────────────────────────
WITH customer_last_order AS (
    SELECT
        f.sk_cliente,
        MAX(t.dt_data) AS last_order_date
    FROM   dw.fato_pedidos  f
    JOIN   dw.dim_tempo     t ON t.sk_tempo = f.sk_tempo
    WHERE  f.ds_status NOT IN ('cancelled', 'returned')
    GROUP BY f.sk_cliente
)
SELECT
    c.sk_cliente,
    c.state,
    clo.last_order_date,
    CURRENT_DATE - clo.last_order_date      AS days_since_last_order,
    CASE
        WHEN CURRENT_DATE - clo.last_order_date > 90 THEN 'churned'
        ELSE 'active'
    END                                     AS churn_status
FROM       customer_last_order  clo
JOIN       dw.dim_clientes      c   ON c.sk_cliente = clo.sk_cliente
ORDER BY   days_since_last_order DESC;


-- ─────────────────────────────────────────────────────────────────────────────
-- Q5: First-purchase cohort analysis
-- Groups customers by the month of their first order and tracks
-- how many are still purchasing each subsequent month.
-- ─────────────────────────────────────────────────────────────────────────────
WITH first_orders AS (
    SELECT
        f.sk_cliente,
        MIN(t.dt_data)                                     AS first_order_date,
        DATE_TRUNC('month', MIN(t.dt_data))                AS cohort_month
    FROM   dw.fato_pedidos  f
    JOIN   dw.dim_tempo     t ON t.sk_tempo = f.sk_tempo
    WHERE  f.ds_status NOT IN ('cancelled', 'returned')
    GROUP BY f.sk_cliente
),
subsequent_orders AS (
    SELECT
        fo.cohort_month,
        DATE_TRUNC('month', t.dt_data)                     AS order_month,
        COUNT(DISTINCT f.sk_cliente)                        AS active_customers
    FROM   dw.fato_pedidos  f
    JOIN   dw.dim_tempo     t  ON t.sk_tempo  = f.sk_tempo
    JOIN   first_orders     fo ON fo.sk_cliente = f.sk_cliente
    WHERE  f.ds_status NOT IN ('cancelled', 'returned')
    GROUP BY fo.cohort_month, DATE_TRUNC('month', t.dt_data)
)
SELECT
    cohort_month,
    order_month,
    active_customers,
    EXTRACT(MONTH FROM AGE(order_month, cohort_month))  AS month_number
FROM   subsequent_orders
ORDER BY cohort_month, order_month;
