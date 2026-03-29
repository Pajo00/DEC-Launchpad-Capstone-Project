{{ config(
    materialized='incremental',
    unique_key='warehouse_id'
) }}

with source as (
    select * from {{ source('raw_db', 'warehouses') }}
    {% if is_incremental() %}
        where ingested_at > (select max(ingested_at) from {{ this }})
    {% endif %}
),

renamed as (
    select
        warehouse_id,
        city,
        state,
        ingested_at
    from source
)

select * from renamed