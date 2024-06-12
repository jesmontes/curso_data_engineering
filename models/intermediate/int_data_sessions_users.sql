{% set event_types = get_values(ref('stg_sql_server_dbo__events'),'event_type') %}

WITH stg_sessions AS (
    SELECT * FROM {{ref('stg_sql_server_dbo__sessions')}}
),

    stg_users AS (
        SELECT * FROM {{ref('stg_sql_server_dbo__users')}}
    ),
    
    joined AS (
    SELECT 
        a.user_id,
        a.session_id,
        b.address_id,
        b.first_name,
        b.last_name,
        b.email,
        a.first_event_time_utc,
        a.last_event_time_utc,
        a.checkout_event_time_utc,
        a.package_shipped_event_time_utc,
        TIMESTAMPDIFF('minute',first_event_time_utc,last_event_time_utc) AS sessions_total_length_min,        
        TIMESTAMPDIFF('minute',first_event_time_utc,checkout_event_time_utc) AS sessions_init_to_checkout_length_min,
        TIMESTAMPDIFF('minute',checkout_event_time_utc,package_shipped_event_time_utc) AS preparing_to_shipped_length_min,        
        TIMESTAMPDIFF('hour',checkout_event_time_utc,package_shipped_event_time_utc) AS preparing_to_shipped_length_hour,
        TIMESTAMPDIFF('day',checkout_event_time_utc,package_shipped_event_time_utc) AS preparing_to_shipped_length_day
    FROM stg_sessions a
    JOIN stg_users b
    ON a.user_id = b.user_id
    GROUP BY ALL--1,2,3,4,5,6,7,8,9,10    
)

SELECT * FROM joined
