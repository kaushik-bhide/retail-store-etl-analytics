with src as (

    -- raw source table from Glue / Athena
    select *
    from {{ source('store_sales', 'fact_orders') }}

),

typed as (

    select
        -- identifiers
        order_id,

        -- customer attributes
        customer_customer_id,
        customer_country,
        customer_segment,

        -- order attributes
        sales_channel,
        currency,
        payment_method,
        payment_status,

        -- numeric standardization
        order_total,

        -- partition columns → integers
        cast(order_year as integer)  as order_year,
        cast(order_month as integer) as order_month,

        -- time helpers
        cast(order_year as integer) * 100
          + cast(order_month as integer) as order_yyyymm,

        date_parse(
          concat(order_year, '-', lpad(order_month, 2, '0'), '-01'),
          '%Y-%m-%d'
        ) as order_month_start

    from src
)

select * from typed
