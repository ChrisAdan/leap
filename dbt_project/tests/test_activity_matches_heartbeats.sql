with activity as (
  select player_id, calendar_date
  from {{ ref('player_activity_daily') }}
),
heartbeats as (
  select distinct player_id, cast(event_datetime as date) as calendar_date
  from {{ ref('event_heartbeat') }}
)
select a.player_id, a.calendar_date
from activity a
left join heartbeats h on a.player_id = h.player_id and a.calendar_date = h.calendar_date
where h.player_id is null