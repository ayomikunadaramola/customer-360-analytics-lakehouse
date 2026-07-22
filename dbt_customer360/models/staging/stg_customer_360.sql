{{ config(materialized='view') }}

select *
from {{ source('customer360_gold', 'customer_360') }}