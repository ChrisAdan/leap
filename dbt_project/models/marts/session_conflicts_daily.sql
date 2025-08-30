{{ config(materialized='table') }}

with conflicts as (
    select
        session_id,
        date_trunc('day', calendar_day::timestamp) as calendar_day,
        sum(daily_conflict_count) as conflict_count,
        sum(total_conflict_seconds) total_conflict_seconds
    from {{ ref('conflict_summary_daily') }}
    group by session_id, calendar_day
)

select * from conflicts
