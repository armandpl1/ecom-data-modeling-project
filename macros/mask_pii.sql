{#- Mask a personal-data column for the Gold layer.

    kind:
      email_hash    SHA-256 of the cleaned email: no address, but the same
                    email always gives the same hash, so it still joins
      email_domain  the part after @ (e.g. gmail.com)
      phone_last4   last 4 digits of the phone number
      initial       first letter followed by a period ("J.")
      zip3          first 3 digits of the ZIP code (an area, not an address)

    Raw values stay in Silver and in restricted.dim_customer_pii. -#}
{% macro mask_pii(column, kind) -%}
    {%- if kind == 'email_hash' -%}
        sha256(lower(trim({{ column }})))
    {%- elif kind == 'email_domain' -%}
        nullif(split_part(lower(trim({{ column }})), '@', 2), '')
    {%- elif kind == 'phone_last4' -%}
        right({{ column }}, 4)
    {%- elif kind == 'initial' -%}
        left({{ column }}, 1) || '.'
    {%- elif kind == 'zip3' -%}
        left({{ column }}, 3)
    {%- else -%}
        {{ exceptions.raise_compiler_error("mask_pii: unknown kind '" ~ kind ~ "'") }}
    {%- endif -%}
{%- endmacro %}
