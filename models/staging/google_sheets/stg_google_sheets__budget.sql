with 

source as (

    select * from {{ source('google_sheets', 'budget') }}

),

renamed as (

    select
        _row,
        quantity,
        month,
        LEFT(month,7) AS YEAR_MONTH,
        product_id,
        _fivetran_synced

    from source

)

select * from renamed
