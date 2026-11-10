{# Use the schema name from the config as is ("silver"), instead of dbt default "main_silver". #}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {{ custom_schema_name if custom_schema_name else target.schema }}
{%- endmacro %}
