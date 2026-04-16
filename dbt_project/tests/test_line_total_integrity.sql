-- test_line_total_integrity.sql
-- ──────────────────────────────
-- Asserts that every order item has a line_total that matches
-- quantity * unit_price within a $0.01 tolerance.
-- Catches upstream ETL rounding issues before they reach marts.

select
    order_item_id,
    order_id,
    product_id,
    quantity,
    unit_price,
    line_total,
    calculated_line_total,
    round(abs(line_total - calculated_line_total), 4) as discrepancy
from {{ ref('stg_order_items') }}
where line_total_matches = false
