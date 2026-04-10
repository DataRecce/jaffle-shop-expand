with rfm_data as (
    select
        customer_id,
        recency_score,
        frequency_score,
        monetary_score,
        rfm_composite_score,
        rfm_category_code,
        recency_days,
        purchase_count,
        lifetime_revenue
    from {{ ref('int_customer_rfm_scores') }}
),

segmented as (
    select
        customer_id,
        recency_score,
        frequency_score,
        monetary_score,
        rfm_composite_score,
        recency_days,
        purchase_count,
        lifetime_revenue,
        case
            when recency_score >= 4 and frequency_score >= 4 and monetary_score >= 4
                then 'Champion'
            when recency_score >= 3 and frequency_score >= 3 and monetary_score >= 3
                then 'Loyal Customer'
            when recency_score >= 4 and frequency_score <= 2
                then 'New Customer'
            when recency_score >= 3 and frequency_score >= 3 and monetary_score <= 2
                then 'Potential Loyalist'
            when recency_score >= 3 and frequency_score <= 2 and monetary_score >= 3
                then 'Big Spender'
            when recency_score <= 2 and frequency_score >= 3 and monetary_score >= 3
                then 'At Risk'
            when recency_score <= 2 and frequency_score >= 4
                then 'Cant Lose Them'
            when recency_score <= 2 and frequency_score <= 2 and monetary_score <= 2
                then 'Lost'
            when recency_score <= 2 and frequency_score >= 2
                then 'Hibernating'
            else 'Need Attention'
        end as customer_segment
    from rfm_data
)

select
    customer_segment,
    count(*) as customer_count,
    round(
        (cast(count(*) as {{ dbt.type_float() }})
        / nullif(sum(count(*)) over (), 0) * 100), 2
    ) as segment_pct,
    round(cast(avg(lifetime_revenue) as numeric), 2) as avg_spend,
    round(cast(avg(purchase_count) as numeric), 2) as avg_orders,
    round(cast(avg(recency_days) as numeric), 0) as avg_days_since_last_order,
    round(cast(avg(rfm_composite_score) as numeric), 2) as avg_rfm_score,
    sum(lifetime_revenue) as total_segment_revenue,
    round(
        (cast(sum(lifetime_revenue) as {{ dbt.type_float() }})
        / nullif(sum(sum(lifetime_revenue)) over (), 0) * 100), 2
    ) as segment_revenue_share_pct
from segmented
group by customer_segment
