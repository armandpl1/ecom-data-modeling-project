{#-
  Column test: fails for every row where the column is below zero.
  Nulls pass (combine with not_null if the column is required).

  Usage:
    columns:
      - name: net_revenue
        data_tests: [not_negative]
-#}
{% test not_negative(model, column_name) %}

select {{ column_name }} as failing_value, *
from {{ model }}
where {{ column_name }} < 0

{% endtest %}
