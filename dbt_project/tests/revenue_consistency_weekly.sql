-- tests/revenue_consistency_weekly.sql
with mart as (
    select country, year_week, round(sum(total_revenue), 2) as mart_revenue
    from {{ ref('country_weekly_revenue') }}
    group by country, year_week
),
raw as (
    select p.country, date_trunc('week', t.eventDateTime::timestamp) as year_week,
    round(sum(t.purchasePrice), 2) as raw_revenue
    from {{ source('leap_raw', 'event_transaction') }} t
    left join {{ source('leap_dim', 'dim_players') }} p
    on t.playerId = p.playerId
    group by country, year_week
)
select mart.country, mart.year_week, mart_revenue, raw_revenue
from mart
join raw using (country, year_week)
where abs(mart_revenue - raw_revenue) > 0.01