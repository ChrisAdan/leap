{{ config(
    materialized='table',
    unique_key='session_id || team_1_id || team_2_id || calendar_day',
    tags=['summary', 'encounter']
) }}

with encounters as (
    select
        session_id,
        team_1_id,
        team_2_id,
        encounter_start,
        encounter_end,
        date(encounter_start) as calendar_day
    from {{ ref('stage_encounters') }}
)

select
    session_id,
    team_1_id,
    team_2_id,
    calendar_day,
    count(*) as daily_encounter_count,
    sum(datediff('second', encounter_start, encounter_end)) as total_encounter_seconds
from encounters
group by session_id, team_1_id, team_2_id, calendar_day
