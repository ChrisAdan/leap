select c.country
from {{ ref('country_weekly_revenue') }} c
left join {{ ref('dim_countries') }} d on c.country = d.country_code
where d.country_code is null
