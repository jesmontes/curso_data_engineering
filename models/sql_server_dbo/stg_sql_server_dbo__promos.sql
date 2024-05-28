{{
  config(
    materialized='view'
  )
}}

WITH src_promos AS (
    SELECT * 
    FROM {{ source('sql_server_dbo', 'promos') }}
    ),

    src_con_no_promo AS (
    SELECT * FROM src_promos

    UNION ALL

    SELECT
        'no-promo' AS promo_id,
        0 AS discount,
        'inactive' AS status,
        null AS _fivetran_deleted,
        current_timestamp AS _fivetran_synced 
    ),

    renamed_casted AS (
        SELECT DISTINCT md5(PROMO_ID) AS PROMO_ID,
                        CONCAT(UPPER(SUBSTRING(PROMO_ID, 1, 1)), LOWER(SUBSTRING(PROMO_ID, 2))) AS description,
                        COALESCE(discount, 0) AS discount_eur,
                        CASE WHEN status = 'active' THEN 0
                            ELSE 1
                        END AS status_id,
                        --status AS status_description,
                        COALESCE(_fivetran_deleted, false) AS _fivetran_deleted,
                        CONVERT_TIMEZONE('UTC',_fivetran_synced) AS _fivetran_synced
                    FROM src_con_no_promo
    )

SELECT * FROM renamed_casted