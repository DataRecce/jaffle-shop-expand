with

orders as (

    select * from {{ ref('stg_orders') }}

),

order_items as (

    select * from {{ ref('order_items') }}

),

order_items_summary as (

    select
        order_id,

        sum(supply_cost) as cost_of_goods,
        sum(product_price) as items_subtotal,
        count(order_item_id) as item_count,
        sum(
            case
                when is_food_item then 1
                else 0
            end
        ) as food_item_count,
        sum(
            case
                when is_drink_item then 1
                else 0
            end
        ) as drink_item_count

    from order_items

    group by 1

),

compute_booleans as (

    select
        orders.*,

        order_items_summary.cost_of_goods,
        order_items_summary.items_subtotal,
        order_items_summary.food_item_count,
        order_items_summary.drink_item_count,
        order_items_summary.item_count,
        order_items_summary.food_item_count > 0 as has_food_items,
        order_items_summary.drink_item_count > 0 as has_drink_items,
        order_items_summary.items_subtotal > 50 as is_high_value_order

    from orders

    left join
        order_items_summary
        on orders.order_id = order_items_summary.order_id

),

customer_order_count as (

    select
        *,

        row_number() over (
            partition by customer_id
            order by ordered_at desc
        ) as customer_order_sequence

    from compute_booleans

)

select * from customer_order_count
