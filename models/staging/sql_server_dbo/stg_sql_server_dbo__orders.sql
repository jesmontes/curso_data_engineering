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
          address_id
          , created_at
          , delivered_at
          , estimated_delivery_at
          , order_cost
          , order_id
          , order_total
          , promo_id
          , shipping_cost
          , shipping_service
          , status
          , tracking_id
          , user_id 
          , _fivetran_deleted
          , _fivetran_synced  
    FROM src_orders
    )

SELECT * FROM renamed_casted