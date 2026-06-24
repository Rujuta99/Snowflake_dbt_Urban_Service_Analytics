select
    sr_number,
    created_date_only as date_day,
    community_area,
    sr_type,
    status,
    origin,
    resolution_hours,
    is_closed,
    has_valid_resolution,
    is_real_service_request,
    has_valid_geo
from {{ ref('stg_sr_311') }}
