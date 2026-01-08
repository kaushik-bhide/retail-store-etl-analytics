with monthly as (

    select
        order_yyyymm,
        order_month_start,
        revenue
    from {{ ref('mart_monthly_revenue') }}

),

with_changes as (

    select
        order_yyyymm,
        order_month_start,
        revenue,

        -- previous month revenue
        lag(revenue) over (order by order_yyyymm) as prev_month_revenue

    from monthly
)

select
    order_yyyymm,
    order_month_start,
    revenue,

    revenue - prev_month_revenue as revenue_change_mom,

    (revenue - prev_month_revenue)
        / nullif(prev_month_revenue, 0) as revenue_growth_rate,

    sum(revenue) over (order by order_yyyymm) as cumulative_revenue

from with_changes

order by order_yyyymm
