{{ config(
    materialized='incremental', 
    unique_key='player_id || calendar_date'
) }}

with sessions as (
    select
        playerId as player_id,
        date_trunc('day'::varchar, eventDateTime::timestamp) as calendar_date,
        eventLengthSeconds,
        kills,
        deaths
    from {{ source('leap_stage', 'fact_session') }}

    {% if is_incremental() %}
      where date_trunc('day'::varchar, eventDateTime::timestamp) > (
        select max(calendar_date) from {{ this }}
      )
    {% endif %}
),

daily_agg as (
    select
        player_id,
        calendar_date,
        sum(eventLengthSeconds) as total_play_time_seconds,
        count(*) as sessions_count,
        sum(kills) as total_kills,
        sum(deaths) as total_deaths,
        case 
          when sum(deaths) = 0 then null
          else round(cast(sum(kills) as float) / sum(deaths), 2)
        end as kill_death_ratio
    from sessions
    group by player_id, calendar_date
)

select * from daily_agg