WITH base_events AS (
    SELECT * 
    FROM {{ ref('base_sql_server_dbo__events') }}
    ),

renamed_casted AS (
    SELECT
         event_id
        ,page_url
        --,event_type
        ,user_id
        product_id
        ,session_id
        ,created_at_utc        
        ,order_id
        , _fivetran_deleted
        ,_fivetran_synced_utc       
    FROM base_events
    )

SELECT * FROM renamed_casted