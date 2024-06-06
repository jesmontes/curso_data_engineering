{{
  config(
    materialized='view'
  )
}}

WITH src_orders AS (
    SELECT * 
    FROM {{ source('sql_server_dbo', 'orders') }}
    ),

status_orders AS (
    SELECT DISTINCT
        md5(status) AS status_orders_id,
        status
        FROM src_orders
                        
)
SELECT * FROM status_orders