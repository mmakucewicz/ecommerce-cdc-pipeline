with items as (
    select * from {{ ref('stg_order_items') }}
),

orders as (
    select * from {{ ref('stg_orders') }}
),

products as (
    select * from {{ ref('stg_products') }}
)

select
    i.order_item_id,
    i.order_id,
    i.product_id,
    p.sku,
    p.product_name,
    p.category,
    i.quantity,
    i.unit_price,
    i.line_amount,
    o.status                   as order_status,
    cast(o.created_at as date) as order_date
from items i
left join orders o   on o.order_id = i.order_id
left join products p on p.product_id = i.product_id
