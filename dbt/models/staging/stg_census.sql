with source as (
    select * from {{ ref('census_socioeconomic') }}
)

select
    try_cast(community_area_number as integer) as community_area,
    community_area_name,
    try_cast(pct_housing_crowded as float)     as pct_housing_crowded,
    try_cast(pct_below_poverty as float)       as pct_below_poverty,
    try_cast(pct_unemployed as float)          as pct_unemployed,
    try_cast(pct_no_hs_diploma as float)       as pct_no_hs_diploma,
    try_cast(pct_under18_over64 as float)      as pct_under18_over64,
    try_cast(per_capita_income as integer)     as per_capita_income,
    try_cast(hardship_index as integer)        as hardship_index
from source
where community_area_number is not null
