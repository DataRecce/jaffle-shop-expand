with

overdue_ar as (
    select
        invoice_id,
        customer_id,
        amount_outstanding,
        due_date,
        extract(day from (current_date - due_date))::integer
    from {{ ref('int_accounts_receivable_aging') }}
    where extract(day from (current_date - due_date))::integer > 90
),

alerts as (
    select
        invoice_id,
        customer_id,
        amount_outstanding,
        due_date,
        extract(day from (current_date - due_date))::integer,
        'ar_overdue_90_days' as alert_type,
        case
            when extract(day from (current_date - due_date))::integer > 180 then 'critical'
            when extract(day from (current_date - due_date))::integer > 120 then 'warning'
            else 'info'
        end as severity
    from overdue_ar
)

select * from alerts
