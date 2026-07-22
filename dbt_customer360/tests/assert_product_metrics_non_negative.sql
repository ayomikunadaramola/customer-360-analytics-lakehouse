select *
from {{ ref('stg_product_performance') }}
where
    units_sold < 0
    or total_orders < 0
    or product_revenue < 0
    or average_selling_price < 0