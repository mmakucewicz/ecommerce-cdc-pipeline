select
    order_date,
    category,
    count(distinct order_id) as orders,
    sum(quantity)            as units,
    sum(line_amount)         as revenue
from {{ ref('fct_order_items') }}
where order_status not in ('cancelled', 'returned')
group by order_date, category
