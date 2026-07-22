{{ config(materialized='table') }}

select
    sales_date,
    total_orders,
    unique_customers,
    gross_order_value,
    average_order_value,
    successful_payments,
    successful_payment_amount,
    unpaid_order_value,
    payment_realisation_rate
from {{ ref('int_sales_performance') }}