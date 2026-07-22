select *
from {{ ref('stg_web_engagement_summary') }}
where
    total_events < 0
    or unique_sessions < 0
    or unique_visitors < 0
    or session_conversion_rate < 0
    or session_conversion_rate > 100