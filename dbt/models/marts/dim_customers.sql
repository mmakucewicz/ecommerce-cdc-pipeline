with customers as (
    select * from {{ ref('stg_customers') }}
),

order_stats as (
    select
        customer_id,
        count(*)                                                  as order_count,
        sum(case when is_revenue then total_amount else 0 end)    as lifetime_value,
        min(ordered_at)                                           as first_order_at,
        max(ordered_at)                                           as last_order_at
    from {{ ref('fct_orders') }}
    group by customer_id
)

select
    c.customer_id,
    c.email,
    c.first_name,
    c.last_name,
    c.city,
    c.country,
    c.created_at                      as registered_at,
    coalesce(s.order_count, 0)        as order_count,
    coalesce(s.lifetime_value, 0)     as lifetime_value,
    s.first_order_at,
    s.last_order_at
from customers c
left join order_stats s on s.customer_id = c.customer_id
