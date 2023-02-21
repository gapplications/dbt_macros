{% macro generate_column(column, flat_out, parent_column_name) %}
    {% if parent_column_name and parent_column_name != column.name %}
        {% set column_name = parent_column_name ~ "." ~ column.name %}
    {% else %}
        {% set column_name = column.name %}
    {% endif %}
    


    {% if column.dtype != 'RECORD' %}
        {%- set data = {
                  'name' : column_name,
                  'rendered_name' : column.name,
                  'type' : column.dtype,
               }
        -%}
        {% do flat_out.update({data.name: data}) %}
    {% endif %}

    {% if column.fields|length > 0 %}
        {% for child_column in column.fields %}
            {% set flat_out = generate_column(child_column, flat_out, parent_column_name=column_name) %}
        {% endfor %}
    {% endif %}
    {% do return(flat_out) %}
{% endmacro %}


{% macro flatten_bigquery(model_name) %}
{% set flat_out= dict() %}


{% set relation=ref(model_name) %}
{%- set columns = adapter.get_columns_in_relation(relation) -%}

{% for column in columns %}
    {% set flat_out = generate_column(column, flat_out, parent_column_name = column.name) %}
{% endfor %}

{% if execute %}

    {% do return(flat_out) %}

{% endif %}

{% endmacro %}