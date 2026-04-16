{{
  config(
    materialized='table',
    tags=['marts', 'finance', 'reporting']
  )
}}

/*
  mart_monthly_revenue
  ────────────────────
  One row per calendar month. Executive-level revenue trend mart.
  Only completed orders are included; cancelled/returned orders
  are counted separately for insight.

  Joins:
    • fct_orders  → order-level financials

  Metrics produced:
    • completed_orders         — number of completed orders in month
    • cancelled_orders         — number of cancelled orders in month
    • returned_orders          — number of returned orders in month
    • gross_revenue            — sum of gross_revenue for completed orders
    • total_discounts          — sum of discount_amount for completed orders
    • total_tax                — sum of tax_amount for completed orders
    • net_revenue              — sum of net_revenue for completed orders
    • avg_order_value          — net_revenue / completed_orders
    • mom_revenue_change_pct   — month-over-month % change in net_revenue
    • cumulative_net_revenue   — running total of net_revenue (year to date within year)
*/

with

orders as (

    select * from {{ ref('fct_orders') }}

),

monthly as (

    select
        order_year,
        order_month,

        -- date spine key
        date_trunc('month', order_date)                                   as month_start_date,

        -- order counts
        count(*) filter (where is_completed)                              as completed_orders,
        count(*) filter (where is_cancelled)                              as cancelled_orders,
        count(*) filter (where is_returned)                               as returned_orders,
        count(*)                                                          as total_orders,

        -- revenue (completed only)
        round(sum(gross_revenue)    filter (where is_completed), 2)       as gross_revenue,
        round(sum(discount_amount)  filter (where is_completed), 2)       as total_discounts,
        round(sum(tax_amount)       filter (where is_completed), 2)       as total_tax,
        round(sum(net_revenue)      filter (where is_completed), 2)       as net_revenue,

        -- distinct customers
        count(distinct customer_id) filter (where is_completed)           as unique_customers,

        -- total items
        sum(total_quantity) filter (where is_completed)                   as total_units_sold

    from orders
    group by 1, 2, 3

),

with_trends as (

    select
        *,

        -- average order value
        case
            when completed_orders = 0 then 0
            else round(net_revenue / completed_orders, 2)
        end                                                               as avg_order_value,

        -- month-over-month change
        round(
            (net_revenue
                - lag(net_revenue) over (order by month_start_date))
            / nullif(lag(net_revenue) over (order by month_start_date), 0)
            * 100
        , 2)                                                              as mom_revenue_change_pct,

        -- year-to-date cumulative revenue (resets each year)
        round(
            sum(net_revenue) over (
                partition by order_year
                order by month_start_date
                rows between unbounded preceding and current row
            )
        , 2)                                                              as ytd_net_revenue

    from monthly

)

select
    order_year,
    order_month,
    month_start_date,
    completed_orders,
    cancelled_orders,
    returned_orders,
    total_orders,
    gross_revenue,
    total_discounts,
    total_tax,
    net_revenue,
    avg_order_value,
    unique_customers,
    total_units_sold,
    mom_revenue_change_pct,
    ytd_net_revenue,
    current_timestamp as _loaded_at
from with_trends
order by month_start_date
