{{ config(
    materialized='incremental',
    unique_key='session_id || event_datetime',
    contract={"enforced": true},
    on_schema_change='fail'
) }}

with source as (
    select rawResponse
    from {{ source('leap_raw', 'event_session')}}
    {% if is_incremental() %}
      -- Only get sessions newer than what we've already processed
      where (rawResponse->>'$.endTime')::timestamp
            > (select max(event_datetime) from {{ this }})
    {% endif %}
),

expanded as (
    select 
        (hb.value->>'$.sessionId')::varchar       as session_id,
        (hb.value->>'$.timestamp')::timestamp       as event_datetime,
        (hb.value->>'$.playerId')::varchar          as player_id,
        (hb.value->>'$.teamId')::varchar            as team_id,
        (hb.value->>'$.positionX')::float           as position_x,
        (hb.value->>'$.positionY')::float           as position_y,
        (hb.value->>'$.positionZ')::float           as position_z
    from source,
         json_each(rawResponse->'$.heartbeats') as hb
)

select
    *,
    now()::timestamp as createdAt
from expanded