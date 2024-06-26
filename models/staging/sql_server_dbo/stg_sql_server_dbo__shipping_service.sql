{{
  config(
    materialized='view'
  )
}}

WITH src_orders AS (
    SELECT * 
    FROM {{ ref('base_sql_server_dbo__orders') }}
    ),
shipping_service AS (
    SELECT DISTINCT 
        {{dbt_utils.generate_surrogate_key(['shipping_service'])}} AS shipping_service_id,
        shipping_service
    FROM src_orders 
)
SELECT * FROM shipping_service