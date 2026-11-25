-- Every observed state of a product, taken from the full CDC history in bronze.
-- Snapshot (r), insert (c) and update (u) events carry the row state in "after".

select
    cast(json_extract_scalar(payload, '$.after.product_id') as bigint)        as product_id,
    json_extract_scalar(payload, '$.after.sku')                               as sku,
    json_extract_scalar(payload, '$.after.name')                              as product_name,
    json_extract_scalar(payload, '$.after.category')                          as category,
    cast(json_extract_scalar(payload, '$.after.price') as decimal(10, 2))     as price,
    source_lsn,
    kafka_offset,
    event_ts
from {{ source('bronze', 'cdc_events') }}
where source_table = 'products'
  and op in ('r', 'c', 'u')
