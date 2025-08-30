{{ config(materialized='table') }}

with sessions as (
    select
        p.country,
        date_trunc('month', s.event_datetime::timestamp) as year_month,
        event_length_seconds as session_seconds
    from {{ source('leap_stage', 'fact_session') }} s
    join {{ source('leap_dim', 'dim_players') }} p on s.player_id = p.player_id
),

monthly_agg as (
    select
        country,
        year_month,
        sum(session_seconds) as total_play_time_seconds
    from sessions
    group by country, year_month
)

select * from monthly_agg