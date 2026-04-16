-- test_no_negative_revenue_on_completed_orders.sql
-- ─────────────────────────────────────────────────
-- Asserts that no completed order has a negative net_revenue.
-- A result set with rows = FAIL; empty result = PASS.

select
    order_id,
    status,
    net_revenue
from {{ ref('fct_orders') }}
where is_completed = true
  and net_revenue < 0
