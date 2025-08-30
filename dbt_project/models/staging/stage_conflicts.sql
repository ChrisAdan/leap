{{ config(
    materialized='incremental',
    unique_key='session_id || team_1_id || team_2_id || conflict_start',
    contract={"enforced": true},
    on_schema_change='fail'
) }}

{{ compute_conflicts(
    centroids_table=ref('stage_centroids'),
    distance_threshold=30,
    cooldown_seconds=60
) }}
