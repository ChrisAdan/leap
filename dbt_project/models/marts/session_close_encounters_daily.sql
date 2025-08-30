{{ config(materialized='table') }}

with encounters as (
    select
        session_id,
        date_trunc('day', calendar_day::timestamp) as calendar_day,
        sum(daily_encounter_count) as close_encounter_count,
        sum(total_encounter_seconds) total_encounter_seconds
    from {{ ref('encounter_summary_daily') }}
    group by session_id, calendar_day
)

select * from encounters
