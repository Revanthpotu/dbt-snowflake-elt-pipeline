{#
  generate_schema_name
  ─────────────────────
  Override dbt's default schema name generation.

  Default dbt behaviour:  <default_schema>_<custom_schema>
    e.g. main_raw_seeds, main_staging, main_marts

  This macro produces clean schema names instead:
    - If a custom_schema_name is provided → use it directly
    - Otherwise → use the default (target schema)

  Works across DuckDB (dev) and Snowflake (prod) without change.
#}

{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ default_schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
