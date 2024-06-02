WITH src_order_items AS (
    SELECT * 
    FROM {{ source('sql_server_dbo', 'order_items') }}
    ),

renamed_casted AS (
    SELECT
        order_id
        ,product_id
        ,quantity
        ,COALESCE(_fivetran_deleted, false) AS _fivetran_deleted
        ,convert_timezone('UTC',_fivetran_synced) AS _fivetran_synced_utc
    FROM src_order_items
    )

SELECT * FROM renamed_casted