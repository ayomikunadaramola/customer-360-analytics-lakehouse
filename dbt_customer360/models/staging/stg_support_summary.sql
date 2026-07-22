{{ config(materialized='view') }}

select *
from {{ source('customer360_gold', 'support_summary') }}