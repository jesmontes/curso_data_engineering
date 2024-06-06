WITH stg_shipping_service AS (
    SELECT * FROM {{ ref('stg_sql_server_dbo__shipping_service') }}
)
SELECT * FROM stg_shipping_service