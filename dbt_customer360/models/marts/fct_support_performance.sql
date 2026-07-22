{{ config(materialized='table') }}

select *
from {{ ref('stg_support_summary') }}