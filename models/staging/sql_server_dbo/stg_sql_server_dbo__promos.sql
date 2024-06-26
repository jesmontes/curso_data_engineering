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
        'no_promo' AS promo_id,
        0 AS discount,
        'inactive' AS status,
        null AS _fivetran_deleted,
        current_timestamp AS _fivetran_synced 
    ),

    renamed_casted AS (
        SELECT DISTINCT 
            {{dbt_utils.generate_surrogate_key(['promo_id'])}} as promo_id,
            PROMO_ID AS description,
            COALESCE(discount, 0) AS discount_eur,
            --TODO status conviene dejarlo mas sencillo con 0-1 o md5 por si a futuro hay muchos
            md5(status) AS status_id,
            /*CASE WHEN status = 'active' THEN 0
                ELSE 1
            END AS status_id,*/
            status,
            COALESCE(_fivetran_deleted, false) AS _fivetran_deleted,
            {{convert_date_timezone('_fivetran_synced')}}
        FROM src_con_no_promo
    )

SELECT * FROM renamed_casted