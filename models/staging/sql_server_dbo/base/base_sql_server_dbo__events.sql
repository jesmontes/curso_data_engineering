WITH src_events AS (
    SELECT * 
    FROM {{ source('sql_server_dbo', 'events') }}
    ),

renamed_casted AS (
    SELECT
        user_id
        ,event_id
        ,page_url
        ,event_type
        ,NULLIF(product_id,'') AS product_id
        ,session_id
        ,{{convert_date_timezone('created_at')}}        
        ,NULLIF(order_id,'') AS order_id
        ,COALESCE(_fivetran_deleted,false) as _fivetran_deleted
        ,{{convert_date_timezone('_fivetran_synced')}}       
    FROM src_events
    )

SELECT * FROM renamed_casted