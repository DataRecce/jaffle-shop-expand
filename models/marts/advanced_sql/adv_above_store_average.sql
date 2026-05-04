-- adv_above_store_average.sql
-- Technique: Correlated Subquery in WHERE clause
-- Finds orders where the amount exceeds the average order total for that store.
-- The correlated subquery recalculates the store average for each row's location_id,
-- which is a classic pattern for threshold-based filtering.

with orders as (

    select * from {{ ref('orders') }}

),

locations as (

    select * from {{ ref('stg_locations') }}

),

-- Correlated subquery: compare each order to its store's average
above_average_orders as (

    select
        o.order_id,
        o.customer_id,
        o.location_id,
        l.location_name,
        o.ordered_at,
        o.order_total,
        o.item_count,
        o.has_food_items,
        o.has_drink_items,

        -- Also fetch the store average for display
        (
            select avg(o2.order_total)
            from orders as o2
            where o2.location_id = o.location_id
        ) as store_avg_order_total

    from orders as o
    inner join locations as l
        on o.location_id = l.location_id

    -- Correlated subquery in WHERE: only keep orders above the store average
    where o.order_total > (
        select avg(o3.order_total)
        from orders as o3
        where o3.location_id = o.location_id
    )

)

select
    order_id,
    customer_id,
    location_id,
    location_name,
    ordered_at,
    order_total,
    item_count,
    has_food_items,
    has_drink_items,
    round(store_avg_order_total, 2) as store_avg_order_total,
    round(order_total - store_avg_order_total, 2) as amount_above_average,
    round(((order_total / nullif(store_avg_order_total, 0)) - 1) * 100, 1) as pct_above_average
from above_average_orders
order by location_id, pct_above_average desc
