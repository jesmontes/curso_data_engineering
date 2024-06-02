{{
  config(
    materialized='view'
  )
}}

WITH src_orders AS (
    SELECT * 
    FROM {{ ref('base_sql_server_dbo__orders') }}
    ),

status_orders AS (
    SELECT DISTINCT
        md5(status) AS status_orders_id,
        status
        FROM src_orders
                        
)
SELECT * FROM status_orders