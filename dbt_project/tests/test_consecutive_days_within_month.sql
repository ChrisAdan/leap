select player_id, year_month, max_consecutive_days_played
from {{ ref('player_consecutive_days_monthly') }}
where max_consecutive_days_played > date_part('day', last_day(year_month))
