-- tests/no_zero_duration_conflicts.sql

with conflicts_with_duration as (
    select
        session_id,
        team_1_id,
        team_2_id,
        conflict_start,
        conflict_end,
        datediff('second', conflict_start, conflict_end) as total_conflict_seconds
    from {{ ref('stage_conflicts') }}  -- replace with the actual ref or table/view name for your conflicts
)

select *
from conflicts_with_duration
where total_conflict_seconds <= 0
