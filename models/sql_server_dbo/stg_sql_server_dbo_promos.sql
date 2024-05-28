{{
  config(
    materialized='view'
  )
}}

WITH src_promos AS (
    SELECT * 
    FROM {{ source('sql_server_dbo', 'promos') }}
    
    UNION ALL

    SELECT
        'no-discount' AS promo_id,
        0 AS discount,
        'active' AS status,
        null AS _fivetran_deleted,
        current_timestamp AS _fivetran_synced 
    ),

    renamed_casted AS (
        SELECT DISTINCT md5(PROMO_ID) AS PROMO_ID,
                        --CONCAT(LEFT(UPPER(PROMO_ID),1), AS description,
                        CONCAT(UPPER(SUBSTRING(PROMO_ID, 1, 1)), LOWER(SUBSTRING(PROMO_ID, 2))) AS description,
                        COALESCE(discount, 0) AS discount,
                        status,
                        COALESCE(_fivetran_deleted, false) AS _fivetran_deleted,
                        _fivetran_synced
                    FROM src_promos
    )

SELECT * FROM renamed_casted