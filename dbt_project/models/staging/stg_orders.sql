{{
  config(
    materialized='view',
    tags=['staging', 'orders']
  )
}}

/*
  stg_orders
  ──────────
  Cleans and standardises raw order header records.

  Transformations applied:
    • Cast column types
    • Normalise order status to lowercase
    • Derive boolean convenience flags (is_completed, is_cancelled, is_returned)
    • Parse date parts for downstream aggregations
*/

with

source as (

    select * from {{ source('raw_seeds', 'raw_orders') }}

),

renamed as (

    select
        -- keys
        cast(order_id as integer)                              as order_id,
        cast(customer_id as integer)                          as customer_id,

        -- status
        lower(trim(status))                                   as status,
        lower(trim(status)) = 'completed'                     as is_completed,
        lower(trim(status)) = 'cancelled'                     as is_cancelled,
        lower(trim(status)) = 'returned'                      as is_returned,
        lower(trim(status)) = 'processing'                    as is_processing,

        -- dates
        cast(order_date as date)                              as order_date,
        date_part('year',  cast(order_date as date))          as order_year,
        date_part('month', cast(order_date as date))          as order_month,
        date_part('quarter', cast(order_date as date))        as order_quarter,

        -- shipping
        trim(shipping_city)                                   as shipping_city,
        upper(trim(shipping_state))                          as shipping_state,
        upper(trim(shipping_country))                        as shipping_country,
        lower(trim(shipping_method))                         as shipping_method,

        -- financials
        coalesce(trim(discount_code), '')                    as discount_code,
        cast(discount_amount as decimal(10,2))               as discount_amount,
        cast(tax_amount as decimal(10,2))                    as tax_amount,
        discount_amount > 0                                   as has_discount,

        -- timestamps
        cast(created_at as timestamp)                        as created_at,
        cast(updated_at as timestamp)                        as updated_at,

        -- metadata
        current_timestamp                                    as _loaded_at,
        '{{ this.name }}'                                    as _dbt_model

    from source

)

select * from renamed
