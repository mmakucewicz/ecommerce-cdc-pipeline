select
    order_id,
    customer_id,
    status,
    created_at,
    updated_at
from {{ source('silver', 'orders') }}
