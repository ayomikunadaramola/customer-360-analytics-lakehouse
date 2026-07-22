{{ config(materialized='view') }}

select *
from {{ source('customer360_gold', 'product_performance') }}