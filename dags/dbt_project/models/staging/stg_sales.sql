{{ config(
    materialized='incremental',
    unique_key='transaction_id'
) }}

with dedup_sales as (
    select * from (
        select
            *,
            row_number() over (
                partition by transaction_id
                order by ingested_at desc
            ) as rn
        from {{ source('raw_db', 'sales') }}
        {% if is_incremental() %}
            where ingested_at > (select max(ingested_at) from {{ this }})
        {% endif %}
    ) as t
    where rn = 1
)

select
    transaction_id,
    store_id,
    product_id,
    quantity_sold::integer             as quantity_sold,
    unit_price::decimal(10,2)          as unit_price,
    discount_pct::decimal(5,2)         as discount_pct,
    sale_amount::decimal(10,2)         as sale_amount,
    transaction_timestamp::timestamp   as transaction_timestamp,
    date(transaction_timestamp)        as sale_date,
    extract(year from transaction_timestamp::timestamp)   as sale_year,
    extract(month from transaction_timestamp::timestamp)  as sale_month,
    extract(dow from transaction_timestamp::timestamp)    as sale_day_of_week,
    round(
        sale_amount::decimal(10,2)
        * (1 - discount_pct::decimal(5,2) / 100), 2
    )                                  as net_sale_amount,
    ingested_at
from dedup_sales