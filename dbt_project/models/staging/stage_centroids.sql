{{ config(
    materialized='incremental',
    unique_key='session_id || team_id || event_datetime',
    contract={"enforced": true},
    on_schema_change='fail'
) }}

with source as (

    select
        session_id,
        team_id,
        event_datetime,
        position_x,
        position_y,
        position_z
    from {{ ref('event_heartbeat') }}

    {% if is_incremental() %}
      where event_datetime > (select max(event_datetime) from {{ this }})
    {% endif %}

),

centroids as (

    select
        session_id,
        team_id,
        event_datetime,
        avg(position_x) as centroid_x,
        avg(position_y) as centroid_y,
        avg(position_z) as centroid_z
    from source
    group by session_id, team_id, event_datetime

)

select
    session_id::string      as session_id,
    team_id::string         as team_id,
    event_datetime::timestamp as event_datetime,
    centroid_x::float       as centroid_x,
    centroid_y::float       as centroid_y,
    centroid_z::float       as centroid_z,
    now()::timestamp        as created_at
from centroids
