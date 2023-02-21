{% macro get_column_names_and_types(table) %}

    {#- Prevent querying of db in parsing mode. This works because this macro does not create any new refs. #}
    {% if not execute %}
        {{ return('') }}
    {% endif %}

    {% set target_relation = adapter.get_relation(database=table.database,
                                          schema=table.schema,
                                         identifier=table.identifier) %}

    {% call statement('get_column_names_and_types', fetch_result=true) %}

        {% if not target_relation and default is none %}

          {{ exceptions.raise_compiler_error("In get_column_names_and_types(): relation " ~ table ~ " does not exist and no default value was provided.") }}

        {% elif not target_relation and default is not none %}

          {{ log("Relation " ~ table ~ " does not exist. Returning the default value: " ~ default) }}

          {{ return(default) }}

        {% else %}

            SELECT
                DISTINCT column_name
                    , data_type
            FROM `{{ table.database }}.{{ table.schema }}.INFORMATION_SCHEMA.COLUMNS`
            WHERE table_name = "{{ table.identifier }}"

        {% endif %}

    {% endcall %}

    {% set value_list = load_result('get_column_names_and_types') %}

    {% if value_list and value_list['data'] %}
        {% set values = value_list['data'] %}
        {{ return(values) }}
    {% else %}
        {{ return(default) }}
    {% endif %}

{% endmacro %}


