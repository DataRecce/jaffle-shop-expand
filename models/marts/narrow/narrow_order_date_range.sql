select
    min(ordered_at) as first_order_date,
    max(ordered_at) as max_date
from {{ ref('stg_orders') }}
