{{ config(
    materialized='incremental',
    unique_key=['warehouse_id', 'product_id', 'snapshot_date'],
    sort=['snapshot_date', 'warehouse_id', 'product_id'],
    sort_type='interleaved'
) }}

with inventory as (
    select * from {{ ref('stg_inventory') }}
),

products as (
    select * from {{ ref('dim_products') }}
),

warehouses as (
    select * from {{ ref('dim_warehouses') }}
),

final as (
    select
        i.snapshot_date,
        i.warehouse_id,
        w.city          as warehouse_city,
        w.state         as warehouse_state,
        i.product_id,
        p.product_name,
        p.category      as product_category,
        p.brand,
        i.quantity_available,
        i.reorder_threshold,
        i.is_low_stock,
        i.ingested_at   as loaded_at
    from inventory i
    left join products p
        on i.product_id = p.product_id
    left join warehouses w
        on i.warehouse_id = w.warehouse_id
)

select * from final

{% if is_incremental() %}
    where loaded_at > (select max(loaded_at) from {{ this }})
{% endif %}