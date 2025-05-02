with
    source as (
        select
            customer_id
            , age
            , gender
            , email
        from {{ source('bootcamp', 'raw_customers') }}
    )

select *
from source

