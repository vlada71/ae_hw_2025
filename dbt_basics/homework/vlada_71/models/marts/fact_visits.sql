WITH customer_feedback AS (
    SELECT *
    FROM
        {{ ref ('stg_haunted_house_tickets')}}
),

tickets AS (
    SELECT *
    FROM
         {{ ref ('stg_haunted_house_tickets')}}
),

-- left join is used since maybe some visitors did not provide a feedback
final AS (

    SELECT 
        tickets.*
        customer_feedback.rating,
        customer_feedback.comments
    FROM 
        tickets
    LEFT JOIN
        customer_feedback
    ON tickets.ticket_id = customer_feedback.ticket_id
)

SELECT 
    ticket_id,
    customer_id,
    haunted_house_id,
    purchase_date,
    visit_date,
    ticket_type,
    ticket_price,
    rating,
    comments
FROM 
    final