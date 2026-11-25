-- SCD Type 2: one row per price version of each product.
-- Built from the complete CDC history in bronze (every change, ordered by WAL position),
-- not from periodic snapshots, so no intermediate price change is lost.

with events as (
    select * from {{ ref('stg_product_price_events') }}
),

with_previous as (
    select
        *,
        lag(price) over (
            partition by product_id
            order by source_lsn, event_ts, kafka_offset
        ) as previous_price
    from events
),

-- Keep only events where the price actually changed
-- (also collapses repeated snapshot events with the same price)
price_changes as (
    select *
    from with_previous
    where previous_price is null
       or previous_price <> price
)

select
    product_id,
    row_number() over (
        partition by product_id
        order by source_lsn, event_ts, kafka_offset
    )                                         as price_version,
    sku,
    product_name,
    category,
    price,
    event_ts                                  as valid_from,
    lead(event_ts) over (
        partition by product_id
        order by source_lsn, event_ts, kafka_offset
    )                                         as valid_to,
    lead(event_ts) over (
        partition by product_id
        order by source_lsn, event_ts, kafka_offset
    ) is null                                 as is_current
from price_changes
