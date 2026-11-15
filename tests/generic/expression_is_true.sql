{#-
  Model test: fails for every row where the SQL expression is not true.
  Use it for business rules that span columns.

  Usage:
    data_tests:
      - expression_is_true:
          arguments:
            expression: "gross_revenue - discount_amount = net_revenue"
-#}
{% test expression_is_true(model, expression) %}

select *
from {{ model }}
where not coalesce(({{ expression }}), false)

{% endtest %}
