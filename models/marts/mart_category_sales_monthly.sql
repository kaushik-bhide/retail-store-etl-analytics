select
    order_year,
    order_month,
    category,

    sum(line_total) as category_revenue,
    sum(quantity)   as units_sold,
    avg(unit_price) as avg_unit_price

from {{ ref('stg_fact_order_items') }}

group by
    order_year,
    order_month,
    category

order by
    order_year,
    order_month,
    category
