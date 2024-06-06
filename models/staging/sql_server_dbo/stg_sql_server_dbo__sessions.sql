WITH sessions AS (
    SELECT 
    user_id,
    session_id,
    MIN(created_at_utc) AS init_session_time,
    MAX (created_at_utc) AS finish_session_time,
    MAX(CASE WHEN event_type = 'checkout' THEN created_at_utc END) AS time_checkout,
    MAX(CASE WHEN event_type = 'package_shipped' THEN created_at_utc END) AS time_package_shipped,
    --TIMESTAMPDIFF('minute',MIN(created_at_utc),MAX(created_at_utc)) as duration_session_min 
    FROM {{ref('base_sql_server_dbo__events')}}
    GROUP BY 1,2
    
)
SELECT * FROM sessions
