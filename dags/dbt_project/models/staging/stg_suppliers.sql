{{ config(
    materialized='incremental',
    unique_key='supplier_id'
) }}

with dedup_suppliers as (
    select * from (
        select
            *,
            row_number() over (
                partition by supplier_id
                order by ingested_at desc
            ) as rn
        from {{ source('raw_db', 'suppliers') }}
        {% if is_incremental() %}
            where ingested_at > (select max(ingested_at) from {{ this }})
        {% endif %}
    ) as t
    where rn = 1
)

select
    supplier_id,
    supplier_name,
    category,
    country,
    ingested_at
from dedup_suppliers