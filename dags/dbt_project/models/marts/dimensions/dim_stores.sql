{{ config(
    materialized='incremental',
    unique_key='store_id'
) }}

with final as (
    select
        store_id,
        store_name,
        city,
        state,
        region,
        store_open_date,
        ingested_at
    from {{ ref('stg_stores') }}
    {% if is_incremental() %}
        where ingested_at > (select max(ingested_at) from {{ this }})
    {% endif %}
)

select * from final