with

source as (

    select * from {{ source('ecom', 'raw_stores') }}

),

renamed as (

    select

        -- primary key: location identifier
        cast(id as varchar) as location_id,

        -- display name for the location
        name as location_name,

        -- tax rate applied at this location (percentage)
        tax_rate,

        -- opening date, truncated to day granularity
        {{ dbt.date_trunc('day', 'opened_at') }} as opened_date

    from source

)

SELECT * FROM renamed
