 {{
  config(
    materialized='view'
  )
}}
WITH src_addresses AS (
    SELECT * 
    FROM {{ source('sql_server_dbo', 'addresses') }}
    ),

addresses_casted AS (     
   SELECT  
            address
            ,address_id
            ,country
            ,state
            ,zipcode
            ,COALESCE(_fivetran_deleted, false) AS _fivetran_deleted
            ,{{convert_date_timezone('_fivetran_synced')}}
        FROM src_addresses
            )

SELECT * FROM addresses_casted