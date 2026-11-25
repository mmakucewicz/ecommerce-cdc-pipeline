-- Cross-layer check: the current SCD2 price (from bronze history) should equal
-- the price in silver (current state). A warning only, because both layers are
-- updated by the same streaming job and can differ for a few seconds.
{{ config(severity='warn') }}

select
    s.product_id,
    s.price          as scd2_price,
    p.current_price  as silver_price
from {{ ref('dim_products_scd2') }} s
join {{ ref('stg_products') }} p on p.product_id = s.product_id
where s.is_current
  and s.price <> p.current_price
