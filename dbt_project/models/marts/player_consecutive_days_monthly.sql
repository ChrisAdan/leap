{{ config(materialized='table') }}

with player_days as (
    select distinct
        player_id,
        date_trunc('day', event_datetime::timestamp) as play_date,
        date_trunc('month', event_datetime::timestamp) as year_month,
        country
    from {{ source('leap_stage', 'fact_session') }}
),

ranked_days as (
    select
        player_id,
        year_month,
        play_date,
        country,
        row_number() over (partition by player_id, year_month order by play_date) as rn
    from player_days
),

groups as (
    select
        player_id,
        year_month,
        play_date,
        country,
        rn,
        date_diff('day', date '1970-01-01', play_date) - rn as grp
    from ranked_days
),

streaks as (
    select
        player_id,
        year_month,
        country,
        grp,
        count(*) as consecutive_days
    from groups
    group by player_id, year_month, country, grp
),

max_streaks as (
    select
        player_id,
        year_month,
        country,
        max(consecutive_days) as max_consecutive_days_played
    from streaks
    group by player_id, year_month, country
)

select * from max_streaks