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
            {{ convert_date_timezone('created_at')}} ,
            {{ convert_date_timezone('delivered_at')}} ,
            {{ convert_date_timezone('estimated_delivery_at')}},
            order_cost AS order_cost_eur,  
            order_id,
            order_total AS order_total_eur,
            md5(COALESCE(NULLIF(promo_id,''),'no_promo')) AS promo_id,
            --COALESCE(NULLIF(promo_id,''),'no_promo') as descripcion,
            shipping_cost AS shipping_cost_eur,
            md5(COALESCE(NULLIF(shipping_service,''),'unknown')) AS shipping_service_id,
            COALESCE(NULLIF(shipping_service,''),'unknown') AS shipping_service,
            md5(status) AS status_id,
            --Para casos de documentacion y problemas de busqueda,se puede valorar dejar descripcion y sea mas facil localizarlo
            --status,
            tracking_id, --TODO poner vacios a null
            user_id,
            COALESCE(_fivetran_deleted, false) AS _fivetran_deleted,
            {{convert_date_timezone('_fivetran_synced')}}
            FROM src_orders
    )

SELECT * FROM renamed_casted

