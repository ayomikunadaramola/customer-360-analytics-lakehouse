{{ config(materialized='view') }}

select *
from {{ source('customer360_gold', 'web_engagement_summary') }}