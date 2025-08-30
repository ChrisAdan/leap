{{ config(
    materialized='incremental',
    unique_key='session_id || event_datetime',
    contract={"enforced": true},
    on_schema_change='fail'
) }}

with source as (
    select raw_response
    from {{ source('leap_raw', 'event_session')}}
    {% if is_incremental() %}
      -- Only get sessions newer than what we've already processed
      where (raw_response ->>'$.end_time')::timestamp
            > (select max(event_datetime) from {{ this }})
    {% endif %}
),

expanded as (
    select 
        (hb.value->>'$.session_id')::varchar       as session_id,
        (hb.value->>'$.timestamp')::timestamp       as event_datetime,
        (hb.value->>'$.player_id')::varchar          as player_id,
        (hb.value->>'$.team_id')::varchar            as team_id,
        (hb.value->>'$.position_x')::float           as position_x,
        (hb.value->>'$.position_y')::float           as position_y,
        (hb.value->>'$.position_z')::float           as position_z
    from source,
         json_each(raw_response->'$.heartbeats') as hb
)

select
    *,
    now()::timestamp as created_at
from expanded