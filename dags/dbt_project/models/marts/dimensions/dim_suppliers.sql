{{ config(
    materialized='incremental',
    unique_key='supplier_id'
) }}

with final as (
    select
        supplier_id,
        supplier_name,
        category,
        country,
        ingested_at
    from {{ ref('stg_suppliers') }}
    {% if is_incremental() %}
        where ingested_at > (select max(ingested_at) from {{ this }})
    {% endif %}
)

select * from final