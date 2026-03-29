{{ config(
    materialized='incremental',
    unique_key='shipment_id'
) }}

with dedup_shipments as (
    select * from (
        select
            *,
            row_number() over (
                partition by shipment_id
                order by ingested_at desc
            ) as rn
        from {{ source('raw_db', 'shipments') }}
        {% if is_incremental() %}
            where ingested_at > (select max(ingested_at) from {{ this }})
        {% endif %}
    ) as t
    where rn = 1
)

select
    shipment_id,
    warehouse_id,
    store_id,
    product_id,
    quantity_shipped::integer            as quantity_shipped,
    shipment_date::date                  as shipment_date,
    expected_delivery_date::date         as expected_delivery_date,
    actual_delivery_date::date           as actual_delivery_date,
    carrier,
    datediff(
        day,
        expected_delivery_date::date,
        actual_delivery_date::date
    )                                    as delivery_delay_days,
    case
        when actual_delivery_date::date > expected_delivery_date::date
        then true
        else false
    end                                  as is_late_delivery,
    case
        when actual_delivery_date::date < expected_delivery_date::date
        then 'early'
        when actual_delivery_date::date = expected_delivery_date::date
        then 'on_time'
        when datediff(day, expected_delivery_date::date,
            actual_delivery_date::date) <= 2
        then 'slightly_late'
        else 'significantly_late'
    end                                  as delivery_status,
    ingested_at
from dedup_shipments