with
    source as (
        select
            feedback_id
            , ticket_id
            , rating
            , comments
        from {{ source('bootcamp', 'raw_customer_feedbacks') }}
    )

select *
from source