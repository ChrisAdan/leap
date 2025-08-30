select *
from {{ ref('country_monthly_playtime') }}
where total_play_time_seconds < 0
