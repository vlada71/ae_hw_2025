with
    customers as (
        select
            customer_id
            , age
            , gender
            , email
        from {{ ref('stg_haunted_customers') }}
    )

    , valid_domains as (
        select valid_domain
        from {{ ref('valid_domains') }}
    )

    , check_valid_emails as (
        select
            customers.customer_id
            , customers.age
            , customers.gender
            , customers.email
            , valid_domains.valid_domain
            , (
                regexp_like(
                    customers.email
                    , '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$'
                ) = true
                and valid_domains.valid_domain is not null
            ) as is_valid_email_address
        from customers
        left join valid_domains on customers.email like '%' || lower(valid_domains.valid_domain)
    )

select * from check_valid_emails