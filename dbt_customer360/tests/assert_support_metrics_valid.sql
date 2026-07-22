select *
from {{ ref('stg_support_summary') }}
where
    total_tickets < 0
    or resolved_tickets < 0
    or open_tickets < 0
    or unresolved_tickets < 0
    or resolution_rate < 0
    or resolution_rate > 100