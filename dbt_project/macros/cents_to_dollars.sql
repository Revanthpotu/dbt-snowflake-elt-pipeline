{# 
  cents_to_dollars(column_name, scale=2)
  ──────────────────────────────────────
  Utility macro to convert integer cent values to decimal dollar amounts.
  Useful when source systems store monetary values as integers (e.g. Stripe).

  Usage:
    {{ cents_to_dollars('amount_cents') }}
    → cast(amount_cents as decimal(18,2)) / 100.0

    {{ cents_to_dollars('amount_cents', scale=4) }}
    → cast(amount_cents as decimal(18,4)) / 100.0
#}

{% macro cents_to_dollars(column_name, scale=2) %}
    cast({{ column_name }} as decimal(18, {{ scale }})) / 100.0
{% endmacro %}
