-- adv_customer_product_array.sql
-- Technique: Array Operations (PostgreSQL-specific)
-- Aggregates each customer's purchase history into a PostgreSQL array.
-- array_agg with DISTINCT and ORDER BY creates a sorted, deduplicated product list.
-- array_length counts unique products without a separate COUNT(DISTINCT).

with order_items as (

    select * from {{ ref('stg_order_items') }}

),

orders as (

    select * from {{ ref('stg_orders') }}

),

products as (

    select * from {{ ref('stg_products') }}

),

customers as (

    select * from {{ ref('stg_customers') }}

),

-- Build arrays of products per customer
customer_product_arrays as (

    select
        c.customer_id,
        c.customer_name,

        -- Array of distinct product IDs purchased, sorted
        array_agg(distinct p.product_id order by p.product_id) as products_purchased,

        -- Array of distinct product names purchased, sorted
        array_agg(distinct p.product_name order by p.product_name) as product_names_purchased,

        -- Array of distinct product types (categories) purchased
        array_agg(distinct p.product_type order by p.product_type) as product_types_purchased,

        -- Count of unique products via array_length
        array_length(array_agg(distinct p.product_id), 1) as unique_product_count,

        -- Total items purchased (including repeats)
        count(oi.order_item_id) as total_items_purchased,

        -- Number of distinct orders
        count(distinct o.order_id) as total_orders

    from order_items as oi
    inner join orders as o
        on oi.order_id = o.order_id
    inner join products as p
        on oi.product_id = p.product_id
    inner join customers as c
        on o.customer_id = c.customer_id
    group by 1, 2

)

select
    customer_id,
    customer_name,
    products_purchased,
    product_names_purchased,
    product_types_purchased,
    unique_product_count,
    total_items_purchased,
    total_orders,
    -- Average items per order
    round(total_items_purchased::numeric / nullif(total_orders, 0), 1) as avg_items_per_order,
    -- Product diversity ratio: unique products / total items
    round(unique_product_count::numeric / nullif(total_items_purchased, 0), 2) as product_diversity_ratio
from customer_product_arrays
order by unique_product_count desc, customer_id
