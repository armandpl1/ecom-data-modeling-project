{#-
  Column test for fact foreign keys: returns the rows that point to the
  dimension's unknown member (macros/unknown_member_key.sql). Use it with
  severity: warn, so an early-arriving fact is visible without failing the
  build; the next incremental run re-points the row once the dimension row
  arrives.

  Usage:
    columns:
      - name: customer_sk
        data_tests:
          - unknown_member_rows:
              config:
                severity: warn
-#}
{% test unknown_member_rows(model, column_name) %}

    select *
    from {{ model }}
    where {{ column_name }} = {{ unknown_member_key() }}

{% endtest %}
