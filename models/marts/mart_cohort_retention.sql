with orders as (

    select
        customer_customer_id,
        order_month_start
    from {{ ref('stg_fact_orders') }}

),

first_orders as (

    select
        customer_customer_id,
        min(order_month_start) as cohort_month
    from orders
    group by customer_customer_id

),

activity as (

    select
        o.customer_customer_id,
        f.cohort_month,
        o.order_month_start,

        date_diff(
            'month',
            f.cohort_month,
            o.order_month_start
        ) as months_since_cohort

    from orders o
    join first_orders f
      on o.customer_customer_id = f.customer_customer_id
)

select
    cohort_month,
    months_since_cohort,
    count(distinct customer_customer_id) as active_customers
from activity
group by cohort_month, months_since_cohort
order by cohort_month, months_since_cohort
