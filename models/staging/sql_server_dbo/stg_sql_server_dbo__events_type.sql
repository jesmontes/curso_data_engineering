WITH events AS (
    SELECT DISTINCT(event_type) AS event_type 
    FROM {{ref('base_sql_server_dbo__events')}}
),
event_type AS (
    SELECT 
        md5(event_type) AS event_type_id,
        event_type AS event_type       
    FROM events
)
SELECT * FROM event_type