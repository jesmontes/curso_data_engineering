{{
  config(
    materialized='view'
  )
}}

WITH src_orders AS (
    SELECT * 
    FROM {{ ref('base_sql_server_dbo__orders') }}
    )
SELECT 
    order_id
    ,shipping_service_id
    ,created_at_utc
    ,delivered_at_utc
    ,estimated_delivery_at_utc
    ,order_cost_eur
    ,order_total_eur
    ,promo_id
    ,shipping_cost_eur
    ,status_id
    ,tracking_id
    ,user_id
    ,_fivetran_deleted
    ,_fivetran_synced_utc

 FROM src_orders