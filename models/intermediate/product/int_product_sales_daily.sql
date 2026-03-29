with

order_items as (

    select * from {{ ref('stg_order_items') }}

),

orders as (

    select * from {{ ref('stg_orders') }}

),

products as (

    select * from {{ ref('stg_products') }}

),

daily_sales as (

    select
        o.ordered_at as sale_date,
        oi.product_id,
        p.product_name,
        p.product_type,
        p.product_price as current_unit_price,
        count(oi.order_item_id) as units_sold,
        count(distinct oi.order_id) as order_count,
        count(oi.order_item_id) * p.product_price as daily_revenue

    from order_items as oi
    inner join orders as o
        on oi.order_id = o.order_id
    inner join products as p
        on oi.product_id = p.product_id
    group by
        o.ordered_at,
        oi.product_id,
        p.product_name,
        p.product_type,
        p.product_price

)

select * from daily_sales
