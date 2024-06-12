WITH stg_events_type AS (
    SELECT * FROM {{ ref('stg_sql_server_dbo__events_type') }}
)
SELECT * FROM stg_events_type