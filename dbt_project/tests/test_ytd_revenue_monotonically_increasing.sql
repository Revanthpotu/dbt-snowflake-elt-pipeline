-- test_ytd_revenue_monotonically_increasing.sql
-- ───────────────────────────────────────────────
-- Asserts that ytd_net_revenue never decreases within the same year.
-- A decrease would indicate a data pipeline bug (e.g. duplicate months
-- or incorrect window frame).

with ordered as (
    select
        order_year,
        order_month,
        month_start_date,
        ytd_net_revenue,
        lag(ytd_net_revenue) over (
            partition by order_year
            order by month_start_date
        ) as prev_ytd_net_revenue
    from {{ ref('mart_monthly_revenue') }}
)

select
    order_year,
    order_month,
    month_start_date,
    prev_ytd_net_revenue,
    ytd_net_revenue
from ordered
where prev_ytd_net_revenue is not null
  and ytd_net_revenue < prev_ytd_net_revenue
