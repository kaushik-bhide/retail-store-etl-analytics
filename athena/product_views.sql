CREATE OR REPLACE VIEW v_cohort_retention AS
WITH first_month AS (
  SELECT
    customer_customer_id AS customer_id,
    MIN(order_month_start) AS cohort_month_start
  FROM v_fact_orders_base
  GROUP BY 1
),
activity AS (
  SELECT DISTINCT
    customer_customer_id AS customer_id,
    order_month_start
  FROM v_fact_orders_base
),
cohort_activity AS (
  SELECT
    f.cohort_month_start,
    a.order_month_start,
    date_diff('month', f.cohort_month_start, a.order_month_start) AS months_since_cohort,
    a.customer_id
  FROM first_month f
  JOIN activity a
    ON f.customer_id = a.customer_id
  WHERE a.order_month_start >= f.cohort_month_start
)
SELECT
  cohort_month_start,
  months_since_cohort,
  COUNT(DISTINCT customer_id) AS active_customers
FROM cohort_activity
GROUP BY 1,2
ORDER BY 1,2;

CREATE OR REPLACE VIEW v_monthly_churn AS
WITH active AS (
  SELECT DISTINCT
    customer_customer_id AS customer_id,
    order_month_start
  FROM v_fact_orders_base
),
seq AS (
  SELECT
    customer_id,
    order_month_start,
    LEAD(order_month_start) OVER (PARTITION BY customer_id ORDER BY order_month_start) AS next_month
  FROM active
),
flags AS (
  SELECT
    order_month_start,
    customer_id,
    CASE
      WHEN next_month IS NULL THEN 1
      WHEN date_diff('month', order_month_start, next_month) > 1 THEN 1
      ELSE 0
    END AS churned_after_month
  FROM seq
),
monthly AS (
  SELECT
    order_month_start,
    COUNT(*) AS active_customers,
    SUM(churned_after_month) AS churned_customers
  FROM flags
  GROUP BY 1
)
SELECT
  order_month_start,
  active_customers,
  churned_customers,
  1.0 * churned_customers / active_customers AS churn_rate
FROM monthly
ORDER BY order_month_start;