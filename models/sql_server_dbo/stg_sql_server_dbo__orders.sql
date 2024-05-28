{{
  config(
    materialized='view'
  )
}}

WITH src_orders AS (
    SELECT * 
    FROM {{ source('sql_server_dbo', 'orders') }}
    ),

    renamed_casted AS (
        SELECT
            CONVERT_TIMEZONE('UTC',created_at) AS created_at,
            CONVERT_TIMEZONE('UTC',delivered_at) AS delivered_at,
            CONVERT_TIMEZONE('UTC',estimated_delivery_at) AS estimated_delivery_at,
            order_cost AS order_cost_eur,  
            order_id,
            order_total AS order_total_eur,
            md5(COALESCE(NULLIF(promo_id,''),'no_promo')) AS promo_id,
            shipping_cost AS shipping_cost_eur,
            COALESCE(NULLIF(shipping_service,''),'unknown') AS shipping_service,
            status,
            tracking_id,
            COALESCE(_fivetran_deleted, false) AS _fivetran_deleted,
            convert_timezone('UTC',_fivetran_synced) AS _fivetran_synced
            FROM src_orders
    )

SELECT * FROM renamed_casted

