{% snapshot budget_snapshot_hard_delete %}

{{
    config(
      target_schema='snapshots',
      unique_key='_row',

      strategy='timestamp',
      updated_at='_fivetran_synced',
      invalidate_hard_deletes=True,
    )
}}

select * from {{ source('google_sheets', 'budget') }}

{% endsnapshot %}