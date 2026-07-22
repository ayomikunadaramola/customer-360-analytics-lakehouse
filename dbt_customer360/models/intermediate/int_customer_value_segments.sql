{{ config(materialized='view') }}

with customers as (

    select *
    from {{ ref('stg_customer_360') }}

),

segmented as (

    select
        customer_id,
        customer_email,
        total_orders,
        total_revenue,
        average_order_value,
        total_payments,
        marketing_events,
        support_tickets,
        web_events,

        case
            when total_revenue >= 5000 then 'High Value'
            when total_revenue >= 2000 then 'Medium Value'
            when total_revenue > 0 then 'Low Value'
            else 'No Revenue'
        end as customer_value_segment,

        case
            when total_orders >= 20 then 'Frequent'
            when total_orders >= 5 then 'Regular'
            when total_orders > 0 then 'Occasional'
            else 'Inactive'
        end as purchase_frequency_segment

    from customers

)

select *
from segmented