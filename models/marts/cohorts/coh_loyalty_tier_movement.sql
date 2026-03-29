with

tier_progression as (

    select * from {{ ref('int_loyalty_tier_progression') }}

),

-- Create monthly snapshots of tier status by using last_activity_at as proxy
member_monthly as (

    select
        loyalty_member_id,
        customer_id,
        current_tier_name,
        earned_tier_name,
        enrolled_at,
        last_activity_at,
        {{ dbt.date_trunc('month', 'last_activity_at') }} as activity_month
    from tier_progression
    where last_activity_at is not null

),

tier_transitions as (

    select
        loyalty_member_id,
        customer_id,
        current_tier_name,
        earned_tier_name,
        activity_month,
        case
            when current_tier_name = earned_tier_name then 'maintained'
            when earned_tier_name is null then 'unknown'
            when current_tier_name != earned_tier_name
                and current_tier_name < coalesce(
                    (select t2.current_tier_name
                     from tier_progression as t2
                     where t2.earned_tier_name = member_monthly.earned_tier_name
                     limit 1),
                    ''
                )
            then 'upgrade_pending'
            else 'maintained'
        end as tier_movement_type
    from member_monthly

),

monthly_summary as (

    select
        activity_month,
        current_tier_name as tier_name,
        count(distinct loyalty_member_id) as member_count,
        count(distinct case when tier_movement_type = 'maintained' then loyalty_member_id end) as maintained_count,
        count(distinct case when tier_movement_type = 'upgrade_pending' then loyalty_member_id end) as upgrade_pending_count
    from tier_transitions
    group by 1, 2

)

select * from monthly_summary
