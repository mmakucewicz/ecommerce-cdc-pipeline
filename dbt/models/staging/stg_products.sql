select
    product_id,
    sku,
    name as product_name,
    category,
    price as current_price,
    is_active
from {{ source('silver', 'products') }}
