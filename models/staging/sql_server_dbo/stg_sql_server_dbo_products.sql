 {{
  config(
    materialized='view'
  )
}}
WITH src_products AS (
    SELECT * 
    FROM {{ source('sql_server_dbo', 'products') }}
    ),


products_casted AS (  
   
   SELECT  product_id,
            --TODO valorar decimal dos decimales
            COALESCE(price,0) AS price_eur,
            name,
            COALESCE(inventory,0) AS inventory,
            COALESCE(_fivetran_deleted, false) AS _fivetran_deleted,
            convert_timezone('UTC',_fivetran_synced) AS _fivetran_synced_utc          
            FROM src_products
            )

SELECT * FROM products_casted