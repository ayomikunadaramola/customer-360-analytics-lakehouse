{{ config(materialized='table') }}

select *
from {{ ref('stg_web_engagement_summary') }}