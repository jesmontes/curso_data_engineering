{{
  config(
    materialized='view'
  )
}}
WITH src_users AS (
    SELECT * 
    FROM {{ source('sql_server_dbo', 'users') }}
    ),
src_orders AS (
    SELECT * 
    FROM {{ ref('base_sql_server_dbo__orders') }}
    ),
total_orders AS (
    SELECT  user_id,
        COUNT(order_total) AS total_orders
        FROM src_orders 
        GROUP BY user_id
        ),

users_renamed AS (
        SELECT 
        u.user_id,
        CONVERT_TIMEZONE('UTC',u.updated_at) AS updated_at_utc,
        u.address_id,
        u.last_name,
        CONVERT_TIMEZONE('UTC',u.created_at) AS created_at_utc,
        u.phone_number,
        --traemos los total_orders de orders y si hay nulos los ponemos a 0
        COALESCE(o.total_orders,0) AS total_orders,
        u.first_name,
        u.email,
        COALESCE(u._fivetran_deleted, false) AS _fivetran_deleted,
        convert_timezone('UTC',_fivetran_synced) AS _fivetran_synced_utc
    FROM src_users u
    JOIN total_orders o
    ON u.user_id = o.user_id
    )
    SELECT * FROM users_renamed

        