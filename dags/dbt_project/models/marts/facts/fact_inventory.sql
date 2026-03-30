{{ config(
    materialized='incremental',
    unique_key=['warehouse_id', 'product_id', 'snapshot_date'],
    sort=['snapshot_date', 'warehouse_id', 'product_id'],
    sort_type='interleaved'
) }}

with inventory as (
    select * from {{ ref('stg_inventory') }}
),

final as (
    select
        i.snapshot_date,
        i.warehouse_id,
        i.product_id,
        i.quantity_available,
        i.reorder_threshold,
        i.is_low_stock,
        i.ingested_at   as loaded_at
    from inventory i
)

select * from final

{% if is_incremental() %}
    where loaded_at > (select max(loaded_at) from {{ this }})
{% endif %}