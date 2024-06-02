WITH src_events AS (
    SELECT * 
    FROM {{ source('sql_server_dbo', 'events') }}
    ),

renamed_casted AS (
    SELECT
         event_id
        ,page_url
        ,event_type
        ,user_id
        ,NULLIF(product_id,'') AS promo_id
        ,session_id
        ,CONVERT_TIMEZONE('UTC',created_at) AS created_at_utc
        ,NULLIF(order_id,'') AS order_id
        ,COALESCE(_fivetran_deleted,false) as _fivetran_deleted
        ,convert_timezone('UTC',_fivetran_synced) AS _fivetran_synced_utc 
    FROM src_events
    )

SELECT * FROM renamed_casted