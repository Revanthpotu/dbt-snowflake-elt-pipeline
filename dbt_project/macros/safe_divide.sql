{#
  safe_divide(numerator, denominator, default=0)
  ───────────────────────────────────────────────
  Divides numerator by denominator, returning `default` when the
  denominator is NULL or zero. Prevents division-by-zero runtime errors.

  Usage:
    {{ safe_divide('total_revenue', 'order_count') }}
    → total_revenue / nullif(order_count, 0)

    {{ safe_divide('profit', 'revenue', default='null') }}
    → case when revenue = 0 or revenue is null then null
           else profit / revenue end
#}

{% macro safe_divide(numerator, denominator, default=0) %}
    case
        when {{ denominator }} is null or {{ denominator }} = 0
            then {{ default }}
        else {{ numerator }} / {{ denominator }}
    end
{% endmacro %}
