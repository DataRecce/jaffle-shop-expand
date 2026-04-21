with


source as (


    select * from {{ source('ecom', 'raw_customers') }}


),


renamed as (

    select

        -- ================= identifiers =================
        cast(id as varchar) as customer_id,


        -- ================= text fields =================
        name as customer_name


    from source

)


select * from renamed
