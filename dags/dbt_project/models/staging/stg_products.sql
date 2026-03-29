{{ config(
    materialized='incremental',
    unique_key='product_id'
) }}

with dedup_products as (
    select * from (
        select
            *,
            row_number() over (
                partition by product_id
                order by ingested_at desc
            ) as rn
        from {{ source('raw_db', 'products') }}
        {% if is_incremental() %}
            where ingested_at > (select max(ingested_at) from {{ this }})
        {% endif %}
    ) as t
    where rn = 1
)

select
    product_id,
    product_name,
    category,
    brand,
    supplier_id,
    unit_price::decimal(10,2) as unit_price,
    ingested_at
from dedup_products