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
            CASE WHEN status = 'active' THEN 0
            ELSE 1
            END AS status_id,
        status 
    FROM src_promos
)
SELECT * FROM status