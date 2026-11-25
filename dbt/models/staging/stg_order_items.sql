select
    order_item_id,
    order_id,
    product_id,
    quantity,
    unit_price,
    quantity * unit_price as line_amount,
    created_at
from {{ source('silver', 'order_items') }}
