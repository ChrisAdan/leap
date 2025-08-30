{{ config(
    materialized='table',
    unique_key='session_id || team_1_id || team_2_id || calendar_day',
    tags=['summary', 'conflict']
) }}

with conflicts as (
    select
        session_id,
        team_1_id,
        team_2_id,
        conflict_start,
        conflict_end,
        date(conflict_start) as calendar_day
    from {{ ref('stage_conflicts') }}
)

select
    session_id,
    team_1_id,
    team_2_id,
    calendar_day,
    count(*) as daily_conflict_count,
    sum(datediff('second', conflict_start, conflict_end)) as total_conflict_seconds
from conflicts
group by session_id, team_1_id, team_2_id, calendar_day
