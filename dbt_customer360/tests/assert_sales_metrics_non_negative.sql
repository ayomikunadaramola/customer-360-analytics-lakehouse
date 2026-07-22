select *
from {{ ref('stg_sales_summary') }}
where
    total_orders < 0
    or unique_customers < 0
    or gross_order_value < 0
    or average_order_value < 0
    or successful_payments < 0
    or successful_payment_amount < 0