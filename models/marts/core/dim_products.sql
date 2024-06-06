WITH stg_products AS (
    SELECT * FROM {{ ref('stg_sql_server_dbo__products') }}
)
SELECT * FROM stg_products
