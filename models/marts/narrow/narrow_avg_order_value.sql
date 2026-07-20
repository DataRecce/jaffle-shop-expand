select
    avg(order_total) as avg_order_value,
    count(*) as order_count
from {{ ref('stg_orders') }}
