{#-
  Unity Catalog governance, applied only on Databricks (no-ops on DuckDB,
  which has no roles or masks).

  - pii_mask(): a SQL function that shows a value only to members of the
    `pii_readers` account group and returns '***' to everyone else.
  - apply_pii_masks(): column masks with that function on every raw
    personal-data column of restricted.dim_customer_pii. Masks follow the
    table, whatever query or tool reads it.
  - grant_read_access(): SELECT on the gold and reporting schemas for all
    account users. restricted gets no grant: owners only.
-#}

{% macro pii_columns() -%}
    {{ return(['first_name', 'last_name', 'full_name', 'email', 'phone_number',
               'phone_extension', 'street_address', 'postal_code']) }}
{%- endmacro %}

{# Databricks runs one statement per call, so each is run on its own #}
{% macro create_pii_mask_function() -%}
    {% if target.type == 'databricks' and execute %}
        {% do run_query("create schema if not exists " ~ target.catalog ~ ".restricted") %}
        {% do run_query(
            "create or replace function " ~ target.catalog ~ ".restricted.pii_mask(value string)"
            ~ " returns string"
            ~ " comment 'Shows personal data only to the pii_readers group'"
            ~ " return case when is_account_group_member('pii_readers') then value else '***' end"
        ) %}
    {% endif %}
    select 1
{%- endmacro %}

{% macro apply_pii_masks(relation) -%}
    {% if target.type == 'databricks' and execute %}
        {% for column in pii_columns() %}
            {% do run_query(
                "alter table " ~ relation ~ " alter column " ~ column
                ~ " set mask " ~ target.catalog ~ ".restricted.pii_mask"
            ) %}
        {% endfor %}
    {% endif %}
    select 1
{%- endmacro %}

{% macro grant_read_access() -%}
    {% if target.type == 'databricks' and execute %}
        {% do run_query("grant use catalog on catalog " ~ target.catalog ~ " to `account users`") %}
        {% for schema in ['gold', 'reporting'] %}
            {% do run_query(
                "grant use schema, select on schema " ~ target.catalog ~ "." ~ schema
                ~ " to `account users`"
            ) %}
        {% endfor %}
    {% endif %}
    select 1
{%- endmacro %}
