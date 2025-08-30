-- tests/no_zero_duration_encounters.sql

with encounters_with_duration as (
    select
        session_id,
        team_1_id,
        team_2_id,
        encounter_start,
        encounter_end,
        datediff('second', encounter_start, encounter_end) as total_encounter_seconds
    from {{ ref('stage_encounters') }}  -- replace with the actual ref or table/view name for your encounters
)

select *
from encounters_with_duration
where total_encounter_seconds <= 0
