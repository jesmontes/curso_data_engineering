WITH base_events AS (
    SELECT * FROM {{ref('base_sql_server_dbo__events')}}
),

    stg_users AS (
        SELECT * FROM {{ref('stg_sql_server_dbo__users')}}
    ),
    
    sessions AS (

        SELECT 
            user_id,
            session_id,
            MIN(created_at_utc) AS first_event_time_utc,
            MAX (created_at_utc) AS last_event_time_utc,
            MAX(CASE WHEN event_type = 'checkout' THEN created_at_utc END) AS checkout_event_time_utc,
            MAX(CASE WHEN event_type = 'package_shipped' THEN created_at_utc END) AS package_shipped_event_time_utc,
        FROM base_events    
        GROUP BY 1,2    
    ),

    joined AS (
        SELECT 
            u.user_id,
            s.session_id,
            u.first_name,
            u.last_name,
            u.address_id,
            u.email,
            s.first_event_time_utc,
            s.last_event_time_utc,
            s.checkout_event_time_utc,
            s.package_shipped_event_time_utc
            FROM stg_users u
            JOIN sessions s
            ON u.user_id = s.user_id
    )

SELECT * FROM joined


       
