with src as (

    select *
    from {{ source('store_sales', 'fact_order_items') }}

),

typed as (

    select
        -- identifiers
        order_id,

        -- partition columns
        cast(order_year as integer)  as order_year,
        cast(order_month as integer) as order_month,

        -- product attributes
        category,
        product_id,

        -- numeric standardization
        cast(quantity as double)   as quantity,
        cast(unit_price as double) as unit_price,
        cast(line_total as double) as line_total

    from src
)

select * from typed
