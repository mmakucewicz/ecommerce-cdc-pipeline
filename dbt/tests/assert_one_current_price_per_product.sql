-- SCD2 integrity: every product must have exactly one current price version.
select
    product_id,
    count(*) as current_versions
from {{ ref('dim_products_scd2') }}
where is_current
group by product_id
having count(*) <> 1
