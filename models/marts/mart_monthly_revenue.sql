select
    order_year,
    order_month,
    order_yyyymm,
    order_month_start,

    count(distinct order_id) as total_orders,
    sum(order_total)         as revenue,
    avg(order_total)         as avg_order_value,

    round(sum(order_total) / 1000000, 2) as revenue_millions

from {{ ref('stg_fact_orders') }}

group by
    order_year,
    order_month,
    order_yyyymm,
    order_month_start

order by
    order_yyyymm
