with orders as (
    select * from {{ ref('stg_orders') }}
),

items as (
    select
        order_id,
        count(*)         as item_count,
        sum(quantity)    as units,
        sum(line_amount) as total_amount
    from {{ ref('stg_order_items') }}
    group by order_id
)

select
    o.order_id,
    o.customer_id,
    o.status,
    cast(o.created_at as date)              as order_date,
    o.created_at                            as ordered_at,
    o.updated_at                            as last_changed_at,
    coalesce(i.item_count, 0)               as item_count,
    coalesce(i.units, 0)                    as units,
    coalesce(i.total_amount, 0)             as total_amount,
    o.status not in ('cancelled', 'returned') as is_revenue
from orders o
left join items i on i.order_id = o.order_id
