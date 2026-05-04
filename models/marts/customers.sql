with

customers as (

    select * from {{ ref('stg_customers') }}

),

orders as (

    select * from {{ ref('orders') }}

),

customer_orders_summary as (

    select
        orders.customer_id,

        count(distinct orders.order_id) as number_of_orders,
        count(distinct orders.order_id) > 1 as has_multiple_orders,
        min(orders.ordered_at) as first_ordered_at,
        max(orders.ordered_at) as last_ordered_at,
        sum(orders.subtotal) as total_spend_before_tax,
        sum(orders.tax_paid) as total_tax_paid,
        sum(orders.order_total) as total_spend

    from orders

    group by 1

),

joined as (

    select
        customers.*,

        customer_orders_summary.number_of_orders,
        customer_orders_summary.first_ordered_at,
        customer_orders_summary.last_ordered_at,
        customer_orders_summary.total_spend_before_tax,
        customer_orders_summary.total_tax_paid,
        customer_orders_summary.total_spend,

        case
            when customer_orders_summary.has_multiple_orders then 'returning'
            else 'new'
        end as buyer_category

    from customers

    left join customer_orders_summary
        on customers.customer_id = customer_orders_summary.customer_id

)

select * from joined
