{% macro camel_to_snake(column_name, active=True) %}

{#-
    WHAT THIS MACRO DOES:
    This macro allows for converting dynamically camelCase column names to snake_case

    HOW TO USE:
    Call this macro in the column AS clause to convert a list of columns to snake_case
    There is a switch to easily turn off the conversion in case the flexibility is needed

 -#}

    {% if active %}
        {% set output = modules.re.sub('([a-z])([A-Z])', '\\1_\\2', column_name) | lower %}
        {{ return(output) }}
    {% else %}
        {{ return(column_name) }}
    {% endif %}

{% endmacro %}
