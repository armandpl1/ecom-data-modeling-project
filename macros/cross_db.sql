{#-
  SQL that differs between DuckDB (the default target) and Databricks.
  Each macro dispatches on the adapter: duckdb__ and databricks__ versions,
  with the DuckDB one producing exactly the SQL the models used before.
  Where one form works on both engines, the models use it directly instead
  (cast(... as string), max_by, qualify, ::, interval arithmetic).
-#}

{# select * without some columns: DuckDB `exclude`, Databricks `except` #}
{% macro star_except(columns, relation=none) -%}
    {{ return(adapter.dispatch('star_except')(columns, relation)) }}
{%- endmacro %}
{% macro duckdb__star_except(columns, relation) -%}
    {{ relation ~ '.' if relation }}* exclude ({{ columns | join(', ') }})
{%- endmacro %}
{% macro databricks__star_except(columns, relation) -%}
    {{ relation ~ '.' if relation }}* except ({{ columns | join(', ') }})
{%- endmacro %}

{# Union of two selects with the same columns in the same order.
   DuckDB matches them by name; Databricks has no BY NAME, so the callers
   only use this for two selects of the same projection. #}
{% macro union_all_same_columns() -%}
    {{ return(adapter.dispatch('union_all_same_columns')()) }}
{%- endmacro %}
{% macro duckdb__union_all_same_columns() -%} union all by name {%- endmacro %}
{% macro databricks__union_all_same_columns() -%} union all {%- endmacro %}

{# yyyymmdd integer key of a date or timestamp (null stays null) #}
{% macro date_key(expr) -%}
    {{ return(adapter.dispatch('date_key')(expr)) }}
{%- endmacro %}
{% macro duckdb__date_key(expr) -%}
    cast(strftime({{ expr }}, '%Y%m%d') as integer)
{%- endmacro %}
{% macro databricks__date_key(expr) -%}
    cast(date_format({{ expr }}, 'yyyyMMdd') as integer)
{%- endmacro %}

{# ISO day of week: Monday 1 ... Sunday 7 #}
{% macro iso_day_of_week(expr) -%}
    {{ return(adapter.dispatch('iso_day_of_week')(expr)) }}
{%- endmacro %}
{% macro duckdb__iso_day_of_week(expr) -%} isodow({{ expr }}) {%- endmacro %}
{% macro databricks__iso_day_of_week(expr) -%}
    cast(extract(dayofweek_iso from {{ expr }}) as bigint)
{%- endmacro %}

{# Calendar days between two dates or timestamps (day boundaries crossed,
   like DuckDB's date_diff('day'); not whole 24-hour periods) #}
{% macro days_between(start, end) -%}
    {{ return(adapter.dispatch('days_between')(start, end)) }}
{%- endmacro %}
{% macro duckdb__days_between(start, end) -%}
    date_diff('day', {{ start }}, {{ end }})
{%- endmacro %}
{% macro databricks__days_between(start, end) -%}
    cast(datediff(cast({{ end }} as date), cast({{ start }} as date)) as bigint)
{%- endmacro %}

{# Calendar months between two dates (month boundaries crossed) #}
{% macro months_between(start, end) -%}
    {{ return(adapter.dispatch('months_between')(start, end)) }}
{%- endmacro %}
{% macro duckdb__months_between(start, end) -%}
    date_diff('month', {{ start }}, {{ end }})
{%- endmacro %}
{% macro databricks__months_between(start, end) -%}
    cast((year({{ end }}) * 12 + month({{ end }})) - (year({{ start }}) * 12 + month({{ start }})) as bigint)
{%- endmacro %}

{# A date plus n months #}
{% macro add_months_to(date_expr, months) -%}
    {{ return(adapter.dispatch('add_months_to')(date_expr, months)) }}
{%- endmacro %}
{% macro duckdb__add_months_to(date_expr, months) -%}
    {{ date_expr }} + to_months({{ months }})
{%- endmacro %}
{% macro databricks__add_months_to(date_expr, months) -%}
    add_months({{ date_expr }}, {{ months }})
{%- endmacro %}

{# Every day from start to end, inclusive, as a one-column table `date_day` #}
{% macro day_spine(start, end) -%}
    {{ return(adapter.dispatch('day_spine')(start, end)) }}
{%- endmacro %}
{% macro duckdb__day_spine(start, end) -%}
    select cast(range as date) as date_day
    from range({{ start }}, {{ end }} + interval 1 day, interval 1 day)
{%- endmacro %}
{% macro databricks__day_spine(start, end) -%}
    select explode(sequence({{ start }}, {{ end }}, interval 1 day)) as date_day
{%- endmacro %}

{# Integers start <= n < stop as a one-column table named `column` #}
{% macro integer_range(start, stop, column) -%}
    {{ return(adapter.dispatch('integer_range')(start, stop, column)) }}
{%- endmacro %}
{% macro duckdb__integer_range(start, stop, column) -%}
    select range as {{ column }} from range({{ start }}, {{ stop }})
{%- endmacro %}
{% macro databricks__integer_range(start, stop, column) -%}
    select id as {{ column }} from range({{ start }}, {{ stop }})
{%- endmacro %}

{# Integer division of whole numbers (both arguments non-negative here) #}
{% macro int_div(numerator, denominator) -%}
    {{ return(adapter.dispatch('int_div')(numerator, denominator)) }}
{%- endmacro %}
{% macro duckdb__int_div(numerator, denominator) -%}
    ({{ numerator }}) // ({{ denominator }})
{%- endmacro %}
{% macro databricks__int_div(numerator, denominator) -%}
    cast(({{ numerator }}) div ({{ denominator }}) as bigint)
{%- endmacro %}

{# Full English month and weekday names, and yyyy-mm #}
{% macro month_name(expr) -%}
    {{ return(adapter.dispatch('month_name')(expr)) }}
{%- endmacro %}
{% macro duckdb__month_name(expr) -%} monthname({{ expr }}) {%- endmacro %}
{% macro databricks__month_name(expr) -%} date_format({{ expr }}, 'MMMM') {%- endmacro %}

{% macro day_name(expr) -%}
    {{ return(adapter.dispatch('day_name')(expr)) }}
{%- endmacro %}
{% macro duckdb__day_name(expr) -%} dayname({{ expr }}) {%- endmacro %}
{% macro databricks__day_name(expr) -%} date_format({{ expr }}, 'EEEE') {%- endmacro %}

{% macro year_month(expr) -%}
    {{ return(adapter.dispatch('year_month')(expr)) }}
{%- endmacro %}
{% macro duckdb__year_month(expr) -%} strftime({{ expr }}, '%Y-%m') {%- endmacro %}
{% macro databricks__year_month(expr) -%} date_format({{ expr }}, 'yyyy-MM') {%- endmacro %}

{# Replace every match of a regular expression #}
{% macro regexp_replace_all(expr, pattern, replacement) -%}
    {{ return(adapter.dispatch('regexp_replace_all')(expr, pattern, replacement)) }}
{%- endmacro %}
{% macro duckdb__regexp_replace_all(expr, pattern, replacement) -%}
    regexp_replace({{ expr }}, {{ pattern }}, {{ replacement }}, 'g')
{%- endmacro %}
{% macro databricks__regexp_replace_all(expr, pattern, replacement) -%}
    regexp_replace({{ expr }}, {{ raw_literal(pattern) }}, {{ replacement }})
{%- endmacro %}

{#- Databricks reads backslashes in '...' as escapes ('\S' would become
    'S'); r'...' keeps a regular expression as written. -#}
{% macro raw_literal(literal) -%}
    {{ 'r' ~ literal if literal.startswith("'") else literal }}
{%- endmacro %}

{#- Every column of this model, in its contract (YAML) order. A select on
    each side of a union written with this lines the columns up by name on
    any engine, so the unknown-member row only names what it sets. -#}
{% macro contract_columns(overrides=none) -%}
    {%- set overrides = overrides or {} -%}
    {%- for name, column in model.columns.items() -%}
        {%- if overrides is not none and name in overrides -%}
            cast({{ overrides[name] }} as {{ column.data_type }}) as {{ name }}
        {%- elif overrides -%}
            cast(null as {{ column.data_type }}) as {{ name }}
        {%- else -%}
            {{ name }}
        {%- endif -%}
        {{ ",\n    " if not loop.last }}
    {%- endfor %}
{%- endmacro %}

{# Lowercase hex SHA-256 of a string #}
{% macro sha256_hex(expr) -%}
    {{ return(adapter.dispatch('sha256_hex')(expr)) }}
{%- endmacro %}
{% macro duckdb__sha256_hex(expr) -%} sha256({{ expr }}) {%- endmacro %}
{% macro databricks__sha256_hex(expr) -%} sha2({{ expr }}, 256) {%- endmacro %}

{# True when the whole string matches the pattern (a SQL string literal) #}
{% macro regexp_full_match(expr, pattern) -%}
    {{ return(adapter.dispatch('regexp_full_match')(expr, pattern)) }}
{%- endmacro %}
{% macro duckdb__regexp_full_match(expr, pattern) -%}
    regexp_full_match({{ expr }}, {{ pattern }})
{%- endmacro %}
{% macro databricks__regexp_full_match(expr, pattern) -%}
    ({{ expr }} rlike concat('^(?:', {{ raw_literal(pattern) }}, ')$'))
{%- endmacro %}

{#- A SQL string literal for text that may hold a quote: DuckDB doubles it
    ('New Year''s Day'), Databricks escapes it with a backslash (it reads two
    quotes as two literals stuck together: 'New Years Day'). -#}
{% macro sql_string(text) -%}
    {{ return(adapter.dispatch('sql_string')(text)) }}
{%- endmacro %}
{% macro duckdb__sql_string(text) -%}
    '{{ text | replace("'", "''") }}'
{%- endmacro %}
{% macro databricks__sql_string(text) -%}
    '{{ text | replace("\\", "\\\\") | replace("'", "\\'") }}'
{%- endmacro %}
