{{
  config(
    materialized='view'
  )
}}

WITH src_promos AS (
    SELECT * 
    FROM {{ source('sql_server_dbo', 'promos') }}
    ),

status AS (
    SELECT 
        DISTINCT
            md5(status) AS status_promo_id,
        status 
    FROM src_promos
)
SELECT * FROM status