{{ config(
    materialized='incremental',
    unique_key='shipment_id',
    sort=['shipment_date', 'warehouse_id', 'store_id'],
    sort_type='interleaved'
) }}

with shipments as (
    select * from {{ ref('stg_shipments') }}
),

products as (
    select * from {{ ref('dim_products') }}
),

final as (
    select
        s.shipment_id,
        s.shipment_date,
        s.warehouse_id,
        s.store_id,
        s.product_id,
        p.supplier_id,    
        s.quantity_shipped,
        s.carrier,
        s.expected_delivery_date,
        s.actual_delivery_date,
        s.delivery_delay_days,
        s.is_late_delivery,
        s.ingested_at       as loaded_at
    from shipments s
    left join products p
        on s.product_id = p.product_id
)

select * from final

{% if is_incremental() %}
    where loaded_at > (select max(loaded_at) from {{ this }})
{% endif %}