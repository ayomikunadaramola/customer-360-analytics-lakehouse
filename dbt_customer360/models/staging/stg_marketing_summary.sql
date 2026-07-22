{{ config(materialized='view') }}

select *
from {{ source('customer360_gold', 'marketing_summary') }}