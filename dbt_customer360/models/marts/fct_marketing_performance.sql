{{ config(materialized='table') }}

select *
from {{ ref('stg_marketing_summary') }}