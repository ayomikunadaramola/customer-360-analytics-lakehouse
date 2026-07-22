{{ config(materialized='table') }}

select
    product_id,
    units_sold,
    total_orders,
    product_revenue,
    average_selling_price,
    revenue_rank,
    units_sold_rank,
    product_sales_status,
    revenue_segment
from {{ ref('int_product_rankings') }}