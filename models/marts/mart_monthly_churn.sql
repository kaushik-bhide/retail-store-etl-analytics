with monthly_activity as (

    select distinct
        customer_customer_id,
        order_month_start
    from {{ ref('stg_fact_orders') }}

),

with_prev as (

    select
        customer_customer_id,
        order_month_start,

        lag(order_month_start)
            over (partition by customer_customer_id order by order_month_start)
            as prev_month

    from monthly_activity
)

select
    order_month_start,

    count(distinct customer_customer_id) as active_customers,

    count(
        distinct case
            when prev_month = date_add('month', -1, order_month_start)
            then customer_customer_id
        end
    ) as retained_customers,

    count(distinct customer_customer_id)
      - count(
            distinct case
                when prev_month = date_add('month', -1, order_month_start)
                then customer_customer_id
            end
        ) as churned_customers,

    (
        count(distinct customer_customer_id)
      - count(
            distinct case
                when prev_month = date_add('month', -1, order_month_start)
                then customer_customer_id
            end
        )
    ) * 1.0 / count(distinct customer_customer_id) as churn_rate

from with_prev
group by order_month_start
order by order_month_start
