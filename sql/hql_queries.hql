-- =============================================================================
-- hql_queries.hql
-- HiveQL equivalents of the analytical queries (Hive / Spark SQL compatible)
-- Assumes tables are registered in Hive metastore or via SparkSession.
-- =============================================================================

-- ─────────────────────────────────────────────────────────────────────────────
-- Q1: Monthly revenue by category (delivered orders only)
-- ─────────────────────────────────────────────────────────────────────────────
SELECT
    t.ano                                                       AS year,
    t.mes                                                       AS month,
    p.category,
    SUM(f.line_total)                                           AS revenue,
    COUNT(DISTINCT f.nk_order_id)                               AS order_count,
    ROUND(SUM(f.line_total) / COUNT(DISTINCT f.nk_order_id), 2) AS avg_ticket
FROM       dw.fato_pedidos   f
JOIN       dw.dim_tempo      t ON t.sk_tempo   = f.sk_tempo
JOIN       dw.dim_produtos   p ON p.sk_produto = f.sk_produto
WHERE      f.ds_status = 'delivered'
GROUP BY   t.ano, t.mes, p.category
ORDER BY   t.ano, t.mes, revenue DESC;


-- ─────────────────────────────────────────────────────────────────────────────
-- Q2: Top 20 best-selling products (by revenue)
-- Hive requires DISTRIBUTE BY / SORT BY for true global sort — use subquery.
-- ─────────────────────────────────────────────────────────────────────────────
SELECT *
FROM (
    SELECT
        p.nk_product_id,
        p.nm_produto,
        p.category,
        SUM(f.quantity)   AS units_sold,
        SUM(f.line_total) AS total_revenue,
        RANK() OVER (ORDER BY SUM(f.line_total) DESC) AS revenue_rank
    FROM   dw.fato_pedidos  f
    JOIN   dw.dim_produtos  p ON p.sk_produto = f.sk_produto
    WHERE  f.ds_status NOT IN ('cancelled', 'returned')
    GROUP BY p.nk_product_id, p.nm_produto, p.category
) ranked
WHERE revenue_rank <= 20
ORDER BY total_revenue DESC;


-- ─────────────────────────────────────────────────────────────────────────────
-- Q3: Average ticket per channel and payment method (last 12 months)
-- Note: Hive date arithmetic uses date_sub / months_add
-- ─────────────────────────────────────────────────────────────────────────────
SELECT
    f.ds_canal                                      AS channel,
    f.method                                        AS payment_method,
    COUNT(DISTINCT f.nk_order_id)                   AS order_count,
    ROUND(AVG(f.vl_total_pedido), 2)                AS avg_ticket,
    ROUND(SUM(f.line_total), 2)                     AS total_revenue
FROM       dw.fato_pedidos  f
JOIN       dw.dim_tempo     t ON t.sk_tempo = f.sk_tempo
WHERE      t.dt_data >= DATE_SUB(CURRENT_DATE, 365)
  AND      f.ds_status NOT IN ('cancelled', 'returned')
GROUP BY   f.ds_canal, f.method
ORDER BY   avg_ticket DESC;


-- ─────────────────────────────────────────────────────────────────────────────
-- Q4: Customer churn indicator
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
    DATEDIFF(CURRENT_DATE, clo.last_order_date) AS days_since_last_order,
    CASE
        WHEN DATEDIFF(CURRENT_DATE, clo.last_order_date) > 90 THEN 'churned'
        ELSE 'active'
    END AS churn_status
FROM       customer_last_order  clo
JOIN       dw.dim_clientes      c ON c.sk_cliente = clo.sk_cliente
ORDER BY   days_since_last_order DESC;


-- ─────────────────────────────────────────────────────────────────────────────
-- Q5: First-purchase cohort analysis
-- ─────────────────────────────────────────────────────────────────────────────
WITH first_orders AS (
    SELECT
        f.sk_cliente,
        MIN(t.dt_data)                                          AS first_order_date,
        TRUNC(MIN(t.dt_data), 'MM')                             AS cohort_month
    FROM   dw.fato_pedidos  f
    JOIN   dw.dim_tempo     t ON t.sk_tempo = f.sk_tempo
    WHERE  f.ds_status NOT IN ('cancelled', 'returned')
    GROUP BY f.sk_cliente
),
subsequent_orders AS (
    SELECT
        fo.cohort_month,
        TRUNC(t.dt_data, 'MM')                                  AS order_month,
        COUNT(DISTINCT f.sk_cliente)                             AS active_customers
    FROM   dw.fato_pedidos  f
    JOIN   dw.dim_tempo     t  ON t.sk_tempo   = f.sk_tempo
    JOIN   first_orders     fo ON fo.sk_cliente = f.sk_cliente
    WHERE  f.ds_status NOT IN ('cancelled', 'returned')
    GROUP BY fo.cohort_month, TRUNC(t.dt_data, 'MM')
)
SELECT
    cohort_month,
    order_month,
    active_customers,
    MONTHS_BETWEEN(order_month, cohort_month) AS month_number
FROM   subsequent_orders
ORDER BY cohort_month, order_month;
