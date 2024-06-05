{{
  config(
    materialized='view'
  )
}}

WITH src_orders AS (
    SELECT * 
    FROM {{ ref('base_sql_server_dbo__orders') }}
    )
SELECT * FROM src_orders