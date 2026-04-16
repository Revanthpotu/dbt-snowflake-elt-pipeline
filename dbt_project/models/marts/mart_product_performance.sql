{{
  config(
    materialized='table',
    tags=['marts', 'products', 'finance']
  )
}}

/*
  mart_product_performance
  ────────────────────────
  One row per product. Aggregates sales performance metrics
  across all completed orders, enriched with product master attributes.

  Joins:
    • stg_products     → product catalog attributes
    • stg_order_items  → line-level sales data
    • stg_orders       → order status filter (completed only)

  Metrics produced:
    • total_units_sold        — sum of quantity across completed orders
    • total_gross_revenue     — sum of line_total across completed orders
    • total_gross_profit      — revenue − COGS
    • realised_margin_pct     — gross profit / revenue * 100
    • distinct_orders         — number of unique completed orders containing this product
    • distinct_customers      — number of unique customers who bought this product
    • revenue_rank            — product ranked by total gross revenue (1 = highest)
*/

with

products as (

    select * from {{ ref('stg_products') }}

),

completed_order_ids as (

    select order_id
    from {{ ref('stg_orders') }}
    where is_completed = true

),

items as (

    select
        oi.product_id,
        oi.order_id,
        oi.quantity,
        oi.unit_price,
        oi.line_total
    from {{ ref('stg_order_items') }} oi
    inner join completed_order_ids co
        on oi.order_id = co.order_id

),

customer_orders as (

    -- needed to join order → customer for distinct_customers
    select
        order_id,
        customer_id
    from {{ ref('stg_orders') }}
    where is_completed = true

),

item_stats as (

    select
        i.product_id,
        sum(i.quantity)                         as total_units_sold,
        sum(i.line_total)                       as total_gross_revenue,
        count(distinct i.order_id)              as distinct_orders,
        count(distinct co.customer_id)          as distinct_customers
    from items i
    left join customer_orders co
        on i.order_id = co.order_id
    group by 1

),

final as (

    select
        -- keys
        p.product_id,
        p.sku,

        -- descriptors
        p.product_name,
        p.category,
        p.subcategory,
        p.is_active,

        -- catalog pricing
        p.unit_price                                                          as catalog_unit_price,
        p.cost_price,
        p.gross_margin_pct                                                    as catalog_margin_pct,

        -- sales performance
        coalesce(s.total_units_sold, 0)                                       as total_units_sold,
        coalesce(round(s.total_gross_revenue, 2), 0)                          as total_gross_revenue,

        -- total COGS = units_sold * cost_price
        coalesce(round(s.total_units_sold * p.cost_price, 2), 0)              as total_cogs,

        -- gross profit
        coalesce(
            round(s.total_gross_revenue - (s.total_units_sold * p.cost_price), 2)
        , 0)                                                                  as total_gross_profit,

        -- realised margin (may differ from catalog if discounts exist at item level)
        case
            when coalesce(s.total_gross_revenue, 0) = 0 then 0
            else round(
                (s.total_gross_revenue - (s.total_units_sold * p.cost_price))
                / s.total_gross_revenue * 100
            , 2)
        end                                                                   as realised_margin_pct,

        -- reach
        coalesce(s.distinct_orders, 0)                                        as distinct_orders,
        coalesce(s.distinct_customers, 0)                                     as distinct_customers,

        -- rank by revenue (dense to handle ties)
        dense_rank() over (
            order by coalesce(s.total_gross_revenue, 0) desc
        )                                                                     as revenue_rank,

        -- metadata
        current_timestamp                                                     as _loaded_at

    from products p
    left join item_stats s
        on p.product_id = s.product_id

)

select * from final
