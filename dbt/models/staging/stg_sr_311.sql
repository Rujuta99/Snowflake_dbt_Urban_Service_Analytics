with source as (
    select * from {{ source('raw', 'sr_311') }}
)

select
    sr_number,
    sr_type,
    sr_short_code,
    owner_department,
    status,
    origin,
    created_ts,
    closed_ts,
    resolution_hours,
    is_closed,
    has_valid_resolution,
    community_area,
    ward,
    zip_code,
    latitude,
    longitude,
    has_valid_geo,
    created_date_only,
    case
        when sr_type ilike '%information only%' then false
        when sr_type ilike '%aircraft noise%' then false
        else true
    end as is_real_service_request
from source
