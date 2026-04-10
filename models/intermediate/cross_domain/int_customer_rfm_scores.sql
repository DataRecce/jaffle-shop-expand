with
c as (
    select * from {{ ref('stg_customers') }}
),

o as (
    select * from {{ ref('stg_orders') }}
),

oi as (
    select * from {{ ref('order_items') }}
),

customer_metrics as (
    select
        c.customer_id,
        {{ dbt.datediff("max(o.ordered_at)", "current_timestamp", "day") }} as recency_days,
        count(distinct o.order_id) as purchase_count,
        coalesce(sum(oi.supply_cost), 0) as lifetime_revenue
    from c
    left join o
        on c.customer_id = o.customer_id
    left join oi
        on o.order_id = oi.order_id
    group by c.customer_id
),

percentiles as (
    select
        customer_id,
        recency_days,
        purchase_count,
        lifetime_revenue,
        ntile(5) over (order by recency_days desc) as recency_score,
        ntile(5) over (order by purchase_count asc) as frequency_score,
        ntile(5) over (order by lifetime_revenue asc) as monetary_score
    from customer_metrics
)

select
    customer_id,
    recency_days,
    purchase_count,
    lifetime_revenue,
    recency_score,
    frequency_score,
    monetary_score,
    recency_score + frequency_score + monetary_score as rfm_composite_score,
    cast(recency_score as {{ dbt.type_string() }})
        || cast(frequency_score as {{ dbt.type_string() }})
        || cast(monetary_score as {{ dbt.type_string() }}) as rfm_category_code
from percentiles
