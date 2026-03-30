{{ config(
    materialized='incremental',
    unique_key='transaction_id',
    sort=['sale_date', 'store_id', 'product_id'],
    sort_type='interleaved'
) }}

with sales as (
    select * from {{ ref('stg_sales') }}
),

final as (
    select
        s.transaction_id,
        s.sale_date,
        s.transaction_timestamp,
        s.store_id,
        s.product_id,
        s.quantity_sold,
        s.unit_price,
        s.discount_pct,
        s.sale_amount,
        s.ingested_at   as loaded_at
    from sales s
)

select * from final

{% if is_incremental() %}
    where loaded_at > (select max(loaded_at) from {{ this }})
{% endif %}