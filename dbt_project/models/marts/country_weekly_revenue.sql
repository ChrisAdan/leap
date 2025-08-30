{{ config(materialized='table') }}

with transactions as (
    select
        p.country,
        date_trunc('week', t.eventDateTime::timestamp) as year_week,
        round(sum(t.purchasePrice), 2) as total_revenue
    from {{ source('leap_raw', 'event_transaction') }} t
    join {{ source('leap_dim', 'dim_players') }} p on t.playerId = p.playerId

    group by p.country, year_week
)
select * from transactions