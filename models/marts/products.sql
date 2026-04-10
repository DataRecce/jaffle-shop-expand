with

products as (

    select * from {{ ref('stg_products') }}

)

select
    product_price,
    product_name,
    is_drink_item,
    product_id,
    product_description,
    is_food_item,
    product_type
from products
