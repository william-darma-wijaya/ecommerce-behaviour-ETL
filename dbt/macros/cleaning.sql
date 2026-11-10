{% macro clean(col) -%}
    nullif(trim({{ col }}), '')
{%- endmacro %}

{# NULL gives false. #}
{% macro is_digits(col) -%}
    coalesce(regexp_full_match({{ col }}, '[0-9]+'), false)
{%- endmacro %}

{% macro batch_id() -%}
    {%- set hint = 'dbt run ... --vars "{batch_id: 2015060100}"' -%}
    {%- if execute and var("batch_id") is none -%}
        {{ exceptions.raise_compiler_error("Pass the batch to process, e.g. " ~ hint) }}
    {%- endif -%}
    '{{ var("batch_id") }}'
{%- endmacro %}
