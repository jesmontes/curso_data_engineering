{{
  config(
    materialized='view'
  )
}}

WITH src_promos AS (
    SELECT * 
    FROM {{ ref('stg_sql_server_dbo__promos') }}
    ),

status AS (
    SELECT 
        DISTINCT
        {{dbt_utils.generate_surrogate_key(['status'])}} AS status_promo_id,
        status 
    FROM src_promos
)
SELECT * FROM status