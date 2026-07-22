select *
from {{ ref('stg_customer_360') }}
where
    total_orders < 0
    or total_revenue < 0
    or average_order_value < 0
    or total_payments < 0
    or marketing_events < 0
    or support_tickets < 0
    or web_events < 0