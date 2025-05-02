with
    tickets as (
        select
            ticket_id
            , customer_id
            , haunted_house_id
            , purchase_date
            , visit_date
            , ticket_type
            , ticket_price
        from {{ ref('stg_haunted_house_tickets') }}
    )

    , feedbacks as (
        select
            ticket_id
            , rating
            , comments
        from {{ ref('stg_customer_feedbacks') }}
    )

    , joining as (
        select
            tickets.ticket_id
            , tickets.customer_id
            , tickets.haunted_house_id
            , tickets.purchase_date
            , tickets.visit_date
            , tickets.ticket_type
            , tickets.ticket_price
            , feedbacks.rating
            , feedbacks.comments
        from tickets
        left join feedbacks on tickets.ticket_id = feedbacks.ticket_id
    )

select *
from joining