{{ config(materialized='table') }}

with player_stats as (
    select
        country,
        playerId as player_id,
        sum(kills) as total_kills,
        sum(deaths) as total_deaths,
        min(eventDateTime) as first_played,
        max(eventDateTime) as last_played
    from {{ source('leap_stage', 'fact_session') }}
    group by country, player_id
),

stats_with_ratio as (
    select
        country,
        player_id,
        total_kills,
        total_deaths,
        case when total_deaths = 0 then null else total_kills*1.0/total_deaths end as kill_death_ratio,
        first_played,
        last_played
    from player_stats
)

select * from stats_with_ratio