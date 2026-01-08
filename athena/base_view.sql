CREATE OR REPLACE VIEW v_fact_orders_base AS
SELECT
  order_id,
  customer_customer_id,
  customer_country,
  customer_segment,
  sales_channel,
  payment_method,
  payment_status,
  currency,
  order_total,
  order_year,
  order_month,

  CAST(order_year AS integer) AS order_year_int,
  CAST(order_month AS integer) AS order_month_int,

  -- 202502 style key for sorting/comparisons
  CAST(order_year AS integer) * 100 + CAST(order_month AS integer) AS order_yyyymm,

  -- real month start date (best for date_diff/window logic)
  date_parse(
    concat(order_year, '-', lpad(order_month, 2, '0'), '-01'),
    '%Y-%m-%d'
  ) AS order_month_start
FROM fact_orders;
