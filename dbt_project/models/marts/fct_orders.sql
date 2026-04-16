{{
  config(
    materialized='table',
    tags=['marts', 'finance', 'orders']
  )
}}

/*
  fct_orders
  ──────────
  One row per order. The central fact table of the marts layer.

  Joins:
    • stg_orders       → header-level facts and flags
    • stg_order_items  → aggregated line-item financials per order
    • stg_customers    → shipping geography denormalisation

  Metrics produced:
    • item_count           — distinct SKUs in the order
    • total_quantity       — total units purchased
    • gross_revenue        — sum of line_total before discounts/tax
    • net_revenue          — gross_revenue − discount_amount + tax_amount
    • average_item_value   — gross_revenue / item_count
*/

with

orders as (

    select * from {{ ref('stg_orders') }}

),

order_items_agg as (

    select
        order_id,
        count(*)                          as item_count,
        sum(quantity)                     as total_quantity,
        sum(line_total)                   as gross_revenue,
        -- check if any line-item had a DQ mismatch
        bool_and(line_total_matches)      as all_items_valid
    from {{ ref('stg_order_items') }}
    group by 1

),

customers as (

    select
        customer_id,
        full_name      as customer_name,
        country        as customer_country,
        state          as customer_state
    from {{ ref('stg_customers') }}

),

final as (

    select
        -- keys
        o.order_id,
        o.customer_id,

        -- customer context
        c.customer_name,
        c.customer_country,
        c.customer_state,

        -- order dates
        o.order_date,
        o.order_year,
        o.order_month,
        o.order_quarter,

        -- order status
        o.status,
        o.is_completed,
        o.is_cancelled,
        o.is_returned,
        o.is_processing,

        -- shipping
        o.shipping_city,
        o.shipping_state,
        o.shipping_country,
        o.shipping_method,

        -- discount
        o.discount_code,
        o.discount_amount,
        o.has_discount,
        o.tax_amount,

        -- line item aggregates
        oi.item_count,
        oi.total_quantity,
        oi.gross_revenue,

        -- net revenue = gross − discount + tax
        round(oi.gross_revenue - o.discount_amount + o.tax_amount, 2)   as net_revenue,

        -- average value per distinct item
        round(oi.gross_revenue / nullif(oi.item_count, 0), 2)           as average_item_value,

        -- data quality flag
        oi.all_items_valid,

        -- metadata
        o.created_at,
        o.updated_at,
        current_timestamp                                                as _loaded_at

    from orders o
    left join order_items_agg oi
        on o.order_id = oi.order_id
    left join customers c
        on o.customer_id = c.customer_id

)

select * from final
