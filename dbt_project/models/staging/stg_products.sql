{{
  config(
    materialized='view',
    tags=['staging', 'products']
  )
}}

/*
  stg_products
  ────────────
  Cleans and standardises raw product catalog records.

  Transformations applied:
    • Cast column types
    • Normalise boolean is_active flag
    • Derive gross_margin_pct for downstream use
    • Standardise category/subcategory casing
*/

with

source as (

    select * from {{ source('raw_seeds', 'raw_products') }}

),

renamed as (

    select
        -- keys
        cast(product_id as integer)                               as product_id,
        trim(sku)                                                 as sku,

        -- descriptors
        trim(product_name)                                        as product_name,
        upper(left(trim(category),1)) || lower(substring(trim(category),2))                                   as category,
        upper(left(trim(subcategory),1)) || lower(substring(trim(subcategory),2))                                as subcategory,

        -- pricing
        cast(unit_price  as decimal(10,2))                       as unit_price,
        cast(cost_price  as decimal(10,2))                       as cost_price,

        -- derived
        round(
            (cast(unit_price as decimal(10,2)) - cast(cost_price as decimal(10,2)))
            / nullif(cast(unit_price as decimal(10,2)), 0) * 100
        , 2)                                                      as gross_margin_pct,

        -- status
        case
            when lower(cast(is_active as varchar)) in ('true', '1', 'yes')
                then true
            else false
        end                                                       as is_active,

        -- timestamps
        cast(created_at as date)                                  as created_at,

        -- metadata
        current_timestamp                                         as _loaded_at,
        '{{ this.name }}'                                         as _dbt_model

    from source

)

select * from renamed
