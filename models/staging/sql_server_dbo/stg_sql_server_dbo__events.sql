{{
  config(
    materialized='view'
  )
}}

WITH src_events AS (
    SELECT * 
    FROM {{ source('sql_server_dbo', 'events') }}
    ),

renamed_casted AS (
    SELECT
            event_id
          , created_at
          , event_type
          , order_id
          , page_url
          , product_id
          , session_id
          , user_id
          , _fivetran_deleted
          , _fivetran_synced AS date_load
    FROM src_events
    )

SELECT * FROM renamed_casted