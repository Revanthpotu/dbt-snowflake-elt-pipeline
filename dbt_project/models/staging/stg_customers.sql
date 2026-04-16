{{
  config(
    materialized='view',
    tags=['staging', 'customers']
  )
}}

/*
  stg_customers
  ─────────────
  Cleans and standardises raw customer records.

  Transformations applied:
    • Cast column types
    • Normalise boolean is_active flag
    • Trim whitespace from name fields
    • Derive full_name convenience column
    • Add dbt metadata columns (loaded_at, model_name)
*/

with

source as (

    select * from {{ source('raw_seeds', 'raw_customers') }}

),

renamed as (

    select
        -- keys
        cast(customer_id as integer)                        as customer_id,

        -- personal info
        trim(first_name)                                    as first_name,
        trim(last_name)                                     as last_name,
        trim(first_name) || ' ' || trim(last_name)         as full_name,
        lower(trim(email))                                  as email,
        phone,

        -- geography
        upper(trim(country))                               as country,
        upper(trim(state))                                 as state,
        trim(city)                                         as city,

        -- status
        case
            when lower(cast(is_active as varchar)) in ('true', '1', 'yes')
                then true
            else false
        end                                                as is_active,

        -- timestamps
        cast(created_at as date)                           as created_at,

        -- metadata
        current_timestamp                                  as _loaded_at,
        '{{ this.name }}'                                  as _dbt_model

    from source

)

select * from renamed
