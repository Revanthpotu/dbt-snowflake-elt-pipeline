{{
  config(
    materialized='table',
    tags=['marts', 'customers']
  )
}}

/*
  dim_customers
  ─────────────
  One row per customer. Customer dimension enriched with lifetime
  order summary statistics — a Type 1 SCD (no history).

  Joins:
    • stg_customers  → customer master attributes
    • fct_orders     → aggregated lifetime order metrics per customer

  Metrics produced:
    • total_orders              — count of all orders placed
    • completed_orders          — count of completed orders only
    • first_order_date          — earliest order date
    • most_recent_order_date    — latest order date
    • lifetime_gross_revenue    — total gross revenue across all completed orders
    • lifetime_net_revenue      — total net revenue across all completed orders
    • avg_order_value           — lifetime_net_revenue / completed_orders
    • customer_segment          — derived RFM-lite segment label
*/

with

customers as (

    select * from {{ ref('stg_customers') }}

),

order_stats as (

    select
        customer_id,
        count(*)                                         as total_orders,
        count(*) filter (where is_completed)             as completed_orders,
        min(order_date)                                  as first_order_date,
        max(order_date)                                  as most_recent_order_date,
        sum(gross_revenue) filter (where is_completed)   as lifetime_gross_revenue,
        sum(net_revenue)   filter (where is_completed)   as lifetime_net_revenue
    from {{ ref('fct_orders') }}
    group by 1

),

final as (

    select
        -- keys
        c.customer_id,

        -- personal
        c.full_name,
        c.first_name,
        c.last_name,
        c.email,

        -- geography
        c.country,
        c.state,
        c.city,

        -- status
        c.is_active,
        c.created_at                                                    as customer_since,

        -- order metrics
        coalesce(os.total_orders, 0)                                    as total_orders,
        coalesce(os.completed_orders, 0)                                as completed_orders,
        os.first_order_date,
        os.most_recent_order_date,
        coalesce(round(os.lifetime_gross_revenue, 2), 0)                as lifetime_gross_revenue,
        coalesce(round(os.lifetime_net_revenue, 2), 0)                  as lifetime_net_revenue,

        -- average order value (completed orders only)
        case
            when coalesce(os.completed_orders, 0) = 0 then 0
            else round(os.lifetime_net_revenue / os.completed_orders, 2)
        end                                                             as avg_order_value,

        -- derived RFM-lite customer segment
        case
            when coalesce(os.completed_orders, 0) = 0
                then 'No Purchases'
            when coalesce(os.lifetime_net_revenue, 0) >= 1000
                and coalesce(os.completed_orders, 0) >= 3
                then 'VIP'
            when coalesce(os.lifetime_net_revenue, 0) >= 500
                then 'High Value'
            when coalesce(os.completed_orders, 0) >= 2
                then 'Repeat Buyer'
            else 'New Customer'
        end                                                             as customer_segment,

        -- metadata
        current_timestamp                                               as _loaded_at

    from customers c
    left join order_stats os
        on c.customer_id = os.customer_id

)

select * from final
