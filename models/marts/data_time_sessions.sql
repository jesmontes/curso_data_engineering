WITH sessions_cte AS (
    SELECT * FROM {{ref('stg_sql_server_dbo__sessions')}}
),
data_time_sessions_cte AS (
   SELECT 
    user_id,
    session_id,
    init_session_time,
    finish_session_time,
    time_checkout,
    time_package_shipped,
    TIMESTAMPDIFF('minute',init_session_time,time_checkout) as sessions_length_init_to_checkout_min,
    TIMESTAMPDIFF('minute',time_checkout,time_package_shipped) as duration_preparing_to_shipped_min,
    TIMESTAMPDIFF('minute',init_session_time,finish_session_time) as sessions_length_total_min,
    TIMESTAMPDIFF('hour',time_checkout,time_package_shipped) as duration_preparing_to_shipped_hour,
    TIMESTAMPDIFF('day',time_checkout,time_package_shipped) as duration_preparing_to_shipped_day
    
    FROM sessions_cte
    
)
SELECT * FROM data_time_sessions_cte
