{#-
  The surrogate key of the "Unknown" row in each dimension (Kimball's unknown
  member). A fact whose customer or product version can't be found yet (an
  early-arriving fact) points here instead of failing the build or holding a
  null key; the unknown_member_rows test warns about every such row.
-#}
{% macro unknown_member_key() -%}
    '-1'
{%- endmacro %}
