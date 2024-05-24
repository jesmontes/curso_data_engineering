{{
  config(
    materialized='view'
  )
}}

WITH src_promos AS (
    SELECT * 
    FROM {{ source('sql_server_dbo', 'promos') }}
    ),

renamed_casted AS (
    SELECT
          discount
          , promo_id
          , status
          , _fivetran_deleted
          , _fivetran_synced 
    FROM src_promos
    )

SELECT * FROM renamed_casted