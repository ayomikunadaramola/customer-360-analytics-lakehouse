{{ config(materialized='table') }}

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
    customer_value_segment,
    purchase_frequency_segment
from {{ ref('int_customer_value_segments') }}