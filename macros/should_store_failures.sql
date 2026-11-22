{#- Overrides dbt's built-in check. On Databricks, failing rows are not
    saved: one view per test (about 240) passes Free Edition's limits of 100
    objects per schema and 500 per metastore. Rerun a failing test there to
    see its rows; on DuckDB they stay in the test_failures schema. -#}
{% macro should_store_failures() %}
    {% if target.type == 'databricks' %}
        {% do return(false) %}
    {% endif %}
    {% set config_store_failures = config.get('store_failures') %}
    {% if config_store_failures is none %}
        {% set config_store_failures = flags.STORE_FAILURES %}
    {% endif %}
    {% do return(config_store_failures) %}
{% endmacro %}
