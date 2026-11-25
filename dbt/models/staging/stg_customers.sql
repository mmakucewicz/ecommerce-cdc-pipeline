select
    customer_id,
    email,
    first_name,
    last_name,
    city,
    country,
    created_at
from {{ source('silver', 'customers') }}
