WITH stg_address AS (
    SELECT * FROM {{ ref('stg_sql_server_dbo__addresses') }}
)
SELECT * FROM stg_address