{{ config(
    materialized='incremental',
    unique_key='transaction_id',
    sort=['sale_date', 'store_id', 'product_id'],
    sort_type='interleaved'
) }}

with sales as (
    select * from {{ ref('stg_sales') }}
),

products as (
    select * from {{ ref('dim_products') }}
),

stores as (
    select * from {{ ref('dim_stores') }}
),

final as (
    select
        s.transaction_id,
        s.sale_date,
        s.transaction_timestamp,
        s.store_id,
        st.store_name,
        st.city         as store_city,
        st.state        as store_state,
        st.region       as store_region,
        s.product_id,
        p.product_name,
        p.category      as product_category,
        p.brand,
        p.supplier_id,
        p.supplier_name,
        s.quantity_sold,
        s.unit_price,
        s.discount_pct,
        s.sale_amount,
        s.ingested_at   as loaded_at
    from sales s
    left join products p
        on s.product_id = p.product_id
    left join stores st
        on s.store_id = st.store_id
)

select * from final

{% if is_incremental() %}
    where loaded_at > (select max(loaded_at) from {{ this }})
{% endif %}