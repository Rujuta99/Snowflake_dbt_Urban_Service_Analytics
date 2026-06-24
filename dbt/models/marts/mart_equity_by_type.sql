-- Resolution time by income quartile, split by service type so we compare
-- like-for-like. Limited to types with enough volume to be meaningful.
with base as (
    select f.sr_type, d.income_quartile, f.resolution_hours
    from {{ ref('fct_service_requests') }} f
    join {{ ref('dim_community_area') }} d on f.community_area = d.community_area
    where f.is_real_service_request
      and f.has_valid_resolution
),

high_volume_types as (
    select sr_type
    from base
    group by sr_type
    having count(*) >= 1000
)

select
    b.sr_type,
    b.income_quartile,
    count(*)                             as request_count,
    round(avg(b.resolution_hours), 1)    as avg_resolution_hours,
    round(median(b.resolution_hours), 1) as median_resolution_hours
from base b
join high_volume_types h on b.sr_type = h.sr_type
group by b.sr_type, b.income_quartile
order by b.sr_type, b.income_quartile
