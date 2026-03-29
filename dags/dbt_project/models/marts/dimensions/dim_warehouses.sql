{{ config(
    materialized='incremental',
    unique_key='warehouse_id'
) }}

with final as (
    select
        warehouse_id,
        city,
        state,
        ingested_at
    from {{ ref('stg_warehouses') }}
    {% if is_incremental() %}
        where ingested_at > (select max(ingested_at) from {{ this }})
    {% endif %}
)

select * from final