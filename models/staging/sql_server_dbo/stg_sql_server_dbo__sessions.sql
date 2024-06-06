WITH sessions AS (
    SELECT 
    user_id,
    session_id,
    MIN(created_at_utc) AS init_time,
    MAX (created_at_utc) AS finish_time,
    TIMESTAMPDIFF('minute',MIN(created_at_utc),MAX(created_at_utc)) as duration_session_min 
    FROM {{ref('stg_sql_server_dbo__events')}}
    GROUP BY 1,2
    
)
SELECT * FROM sessions
