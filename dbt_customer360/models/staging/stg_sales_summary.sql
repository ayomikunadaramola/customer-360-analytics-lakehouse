{{ config(materialized='view') }}

select *
from {{ source('customer360_gold', 'sales_summary') }}