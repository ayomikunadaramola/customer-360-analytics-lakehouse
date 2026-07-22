{{ config(materialized='view') }}

with products as (

    select *
    from {{ ref('stg_product_performance') }}

),

ranked as (

    select
        product_id,
        units_sold,
        total_orders,
        product_revenue,
        average_selling_price,
        revenue_rank,
        product_sales_status,

        dense_rank() over (
            order by units_sold desc
        ) as units_sold_rank,

        case
            when product_revenue >= 10000 then 'Top Revenue'
            when product_revenue >= 5000 then 'Strong Revenue'
            when product_revenue > 0 then 'Low Revenue'
            else 'No Revenue'
        end as revenue_segment

    from products

)

select *
from ranked