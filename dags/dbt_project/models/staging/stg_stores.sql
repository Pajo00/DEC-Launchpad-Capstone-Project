{{ config(
    materialized='incremental',
    unique_key='store_id'
) }}

with dedup_stores as (
    select * from (
        select
            *,
            row_number() over (
                partition by store_id
                order by ingested_at desc
            ) as rn
        from {{ source('raw_db', 'stores') }}
        {% if is_incremental() %}
            where ingested_at > (select max(ingested_at) from {{ this }})
        {% endif %}
    ) as t
    where rn = 1
)

select
    store_id,
    store_name,
    city,
    state,
    region,
    to_date(store_open_date, 'DD/MM/YYYY') as store_open_date,
    ingested_at
from dedup_stores