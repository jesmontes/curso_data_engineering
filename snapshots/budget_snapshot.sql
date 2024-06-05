{% snapshot budget_snapshot %}

{{
    config(
      target_schema='snapshots',
      unique_key='_row',
      strategy='timestamp',
      updated_at='_fivetran_synced',
    )
}}

select * from {{ source('google_sheets', 'budget') }}
    WHERE _fivetran_synced > (SELECT NVL(MAX(_fivetran_synced),'2000-01-01') FROM {{ this }} )


{% endsnapshot %}