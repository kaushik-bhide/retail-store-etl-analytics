CREATE OR REPLACE VIEW v_monthly_revenue AS
SELECT
  order_year_int AS order_year,
  order_month_int AS order_month,
  order_yyyymm,
  COUNT(*) AS orders,
  SUM(order_total) AS raw_revenue,
  format('%.2fM', SUM(order_total) / 1000000) AS revenue_millions,
  ROUND(AVG(order_total),2) AS avg_order_value
FROM v_fact_orders_base
GROUP BY 1,2,3
ORDER BY 1,2;

CREATE OR REPLACE VIEW v_revenue_mom AS
WITH m AS (
  SELECT
    order_yyyymm,
    SUM(order_total) AS revenue
  FROM v_fact_orders_base
  GROUP BY 1
)
SELECT
  order_yyyymm,
  format('%,.0f', revenue) As revenue,
  format('%,.0f', revenue - LAG(revenue) OVER (ORDER BY order_yyyymm)) AS revenue_change,
  1.0 * (revenue - LAG(revenue) OVER (ORDER BY order_yyyymm))
      / LAG(revenue) OVER (ORDER BY order_yyyymm) AS revenue_growth_rate,
  format('%,.0f', SUM(revenue) OVER (ORDER BY order_yyyymm)) AS running_revenue
FROM m
ORDER BY order_yyyymm;

CREATE OR REPLACE VIEW v_category_sales_monthly AS
SELECT
  CAST(order_year AS integer) AS order_year,
  CAST(order_month AS integer) AS order_month,
  category,
  format('%,.0f', SUM(line_total)) AS category_revenue,
  SUM(quantity) AS units_sold
FROM fact_order_items
GROUP BY 1,2,3
ORDER BY units_sold DESC;