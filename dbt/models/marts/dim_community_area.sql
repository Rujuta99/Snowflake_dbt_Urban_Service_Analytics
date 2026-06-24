select
    community_area,
    community_area_name,
    per_capita_income,
    hardship_index,
    pct_below_poverty,
    pct_unemployed,
    ntile(4) over (order by per_capita_income) as income_quartile
from {{ ref('stg_census') }}
