{{
  config(
    materialized='view',
    tags=['staging', 'orders']
  )
}}

/*
  stg_order_items
  ───────────────
  Cleans and standardises raw order line-item records.

  Transformations applied:
    • Cast column types
    • Validate / recalculate line_total for data quality
    • Expose a recalculated_line_total to catch upstream drift
*/

with

source as (

    select * from {{ source('raw_seeds', 'raw_order_items') }}

),

renamed as (

    select
        -- keys
        cast(order_item_id as integer)                           as order_item_id,
        cast(order_id as integer)                               as order_id,
        cast(product_id as integer)                             as product_id,

        -- measures
        cast(quantity as integer)                               as quantity,
        cast(unit_price as decimal(10,2))                       as unit_price,
        cast(line_total as decimal(10,2))                       as line_total,

        -- derived / data-quality check
        cast(quantity as integer)
            * cast(unit_price as decimal(10,2))                 as calculated_line_total,

        abs(
            cast(line_total as decimal(10,2))
            - (cast(quantity as integer) * cast(unit_price as decimal(10,2)))
        ) < 0.01                                                as line_total_matches,

        -- metadata
        current_timestamp                                       as _loaded_at,
        '{{ this.name }}'                                       as _dbt_model

    from source

)

select * from renamed
