{{
  config(
    materialized='view'
  )
}}

WITH src_users AS (
    SELECT * 
    FROM {{ source('sql_server_dbo', 'users') }}
    ),

renamed_casted AS (
    SELECT
           address_id
          , created_at
          , email
          , first_name
          , last_name
          , phone_number
          , total_orders
          , updated_at
          , user_id
          , _fivetran_deleted
          , _fivetran_synced  AS date_load
    FROM src_users
    )

SELECT * FROM renamed_casted