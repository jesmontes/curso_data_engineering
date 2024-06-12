WITH stg_orders_status AS (
    SELECT * FROM {{ ref('stg_sql_server_dbo__orders_status') }}
)
SELECT * FROM stg_orders_status