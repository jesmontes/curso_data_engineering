WITH stg_promos_status AS (
    SELECT * FROM {{ ref('stg_sql_server_dbo__promos_status') }}
)
SELECT * FROM stg_promos_status
