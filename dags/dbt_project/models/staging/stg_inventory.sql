{{ config(
    materialized='incremental',
    unique_key=['warehouse_id', 'product_id', 'snapshot_date']
) }}

with dedup_inventory as (
    select * from (
        select
            *,
            row_number() over (
                partition by warehouse_id, product_id, snapshot_date
                order by ingested_at desc
            ) as rn
        from {{ source('raw_db', 'inventory') }}
        {% if is_incremental() %}
            where ingested_at > (select max(ingested_at) from {{ this }})
        {% endif %}
    ) as t
    where rn = 1
)

select
    warehouse_id,
    product_id,
    quantity_available::integer   as quantity_available,
    reorder_threshold::integer    as reorder_threshold,
    snapshot_date::date           as snapshot_date,
    case
        when quantity_available <= reorder_threshold then true
        else false
    end                           as is_low_stock,
    round(
        (quantity_available - reorder_threshold) * 100.0
        / nullif(reorder_threshold, 0), 2
    )                             as stock_buffer_pct,
    ingested_at
from dedup_inventory