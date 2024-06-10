{% set event_types = get_values(ref('stg_sql_server_dbo__events'),'event_type') %}

WITH stg_events AS (

        SELECT * FROM {{ref('base_sql_server_dbo__events')}}
),

    stg_sessions AS (

        SELECT  
            user_id,
            session_id,
            first_event_time_utc,
            last_event_time_utc,
            checkout_event_time_utc,
            package_shipped_event_time_utc,
            TIMESTAMPDIFF('minute',first_event_time_utc,last_event_time_utc) AS sessions_length_total_min,        
            TIMESTAMPDIFF('minute',first_event_time_utc,checkout_event_time_utc) AS sessions_length_init_to_checkout_min,
            TIMESTAMPDIFF('minute',checkout_event_time_utc,package_shipped_event_time_utc) AS preparing_to_shipped_length_time_min,        
            TIMESTAMPDIFF('hour',checkout_event_time_utc,package_shipped_event_time_utc) AS duration_preparing_to_shipped_hour,
            TIMESTAMPDIFF('day',checkout_event_time_utc,package_shipped_event_time_utc) AS duration_preparing_to_shipped_day
        FROM {{ref('stg_sql_server_dbo__sessions')}}
        GROUP BY 1,2,3,4,5,6
    ),

    stg_users AS (
        SELECT 
            user_id,
            first_name,
            last_name,
            email
        FROM {{ref('stg_sql_server_dbo__users')}}
    ),

    stg_events_by_user_amount AS (
        SELECT 
            user_id,
            session_id,
            {%- for event_type in event_types   %}
            sum(case when event_type = '{{event_type}}' then 1 end) AS {{event_type}}_amount
            {%- if not loop.last %},{% endif -%}
            {% endfor %}
        FROM stg_events
        GROUP BY 1,2

    ),

    joined AS (
        SELECT * 
        FROM stg_users A
        JOIN stg_sessions B
        ON a.user_id = b.user_id
        JOIN stg_events C
        ON c.user_id = b.user_id
    )

SELECT * FROM joined
--     sessions AS (
--         SELECT 
--         user_id,
--         session_id,
--         first_event_time_utc,
--         last_event_time_utc,
--         checkout_event_time_utc,
--         package_shipped_event_time_utc,
--         TIMESTAMPDIFF('minute',first_event_time_utc,last_event_time_utc) AS sessions_length_total_min,        
--         TIMESTAMPDIFF('minute',init_session_time,checkout_event_time_utc) AS sessions_length_init_to_checkout_min,
--         TIMESTAMPDIFF('minute',checkout_event_time_utc,package_shipped_event_time_utc) AS preparing_to_shipped_length_time_min,        
--         TIMESTAMPDIFF('hour',checkout_event_time_utc,package_shipped_event_time_utc) AS duration_preparing_to_shipped_hour,
--         TIMESTAMPDIFF('day',checkout_event_time_utc,package_shipped_event_time_utc) AS duration_preparing_to_shipped_day,
--         {%- for event_type in event_types   %}
--         sum(case when event_type = '{{event_type}}' then 1 end) AS {{event_type}}_amount
--         {%- if not loop.last %},{% endif -%}
--         {% endfor %}
--         FROM stg_events
--         GROUP BY 1,2
    
-- )


