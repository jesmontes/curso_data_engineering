{% set event_types = get_values(ref('stg_sql_server_dbo__events'),'event_type') %}

WITH sessions AS (

    SELECT 
        user_id,
        session_id,
        MIN(created_at_utc) AS first_event_time_utc,
        MAX (created_at_utc) AS last_event_time_utc,
        MAX(CASE WHEN event_type = 'checkout' THEN created_at_utc END) AS checkout_event_time_utc,
        MAX(CASE WHEN event_type = 'package_shipped' THEN created_at_utc END) AS package_shipped_event_time_utc,
        {%- for event_type in event_types   %}
            sum(case when event_type = '{{event_type}}' then 1 end) AS {{event_type}}_amount
            {%- if not loop.last %},{% endif -%}
        {% endfor %}
    FROM {{ref('base_sql_server_dbo__events')}}
    GROUP BY 1,2
    
)

SELECT * FROM sessions
       
