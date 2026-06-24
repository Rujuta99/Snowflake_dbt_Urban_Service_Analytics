select
    sr_type,
    max(sr_short_code)                  as sr_short_code,
    max(owner_department)               as owner_department,
    boolor_agg(is_real_service_request) as is_real_service_request
from {{ ref('stg_sr_311') }}
group by sr_type
