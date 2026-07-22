{{ config(materialized='view') }}

with sales as (

    select *
    from {{ ref('stg_sales_summary') }}

),

enriched as (

    select
        sales_date,
        total_orders,
        unique_customers,
        gross_order_value,
        average_order_value,
        successful_payments,
        successful_payment_amount,

        gross_order_value
            - successful_payment_amount
            as unpaid_order_value,

        case
            when gross_order_value > 0 then
                round(
                    successful_payment_amount
                    / gross_order_value * 100,
                    2
                )
            else 0
        end as payment_realisation_rate

    from sales

)

select *
from enriched