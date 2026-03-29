{{ config(
    materialized='incremental',
    unique_key='product_id'
) }}

with products as (
    select * from {{ ref('stg_products') }}
    {% if is_incremental() %}
        where ingested_at > (select max(ingested_at) from {{ this }})
    {% endif %}
),

suppliers as (
    select * from {{ ref('stg_suppliers') }}
),

final as (
    select
        p.product_id,
        p.product_name,
        p.category,
        p.brand,
        p.unit_price,
        s.supplier_id,
        s.supplier_name,
        s.country       as supplier_country,
        p.ingested_at
    from products p
    left join suppliers s
        on p.supplier_id = s.supplier_id
)

select * from final