select
    d.income_quartile,
    count(*)                             as request_count,
    round(avg(f.resolution_hours), 1)    as avg_resolution_hours,
    round(median(f.resolution_hours), 1) as median_resolution_hours
from {{ ref('fct_service_requests') }} f
join {{ ref('dim_community_area') }} d on f.community_area = d.community_area
where f.is_real_service_request
  and f.has_valid_resolution
group by 1
order by 1
