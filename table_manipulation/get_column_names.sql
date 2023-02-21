{% macro get_column_names(table, fields_to_ignore) -%}

    {#-- Prevent querying of db in parsing mode. This works because this macro does not create any new refs. #}
    {%- if not execute -%}
        {{ return('') }}
    {% endif %}

    {%- set target_relation = adapter.get_relation(database=table.database,
                                          schema=table.schema,
                                         identifier=table.identifier) -%}

    {%- call statement('get_column_names', fetch_result=true) %}

        {%- if not target_relation and default is none -%}

          {{ exceptions.raise_compiler_error("In get_column_names(): relation " ~ table ~ " does not exist and no default value was provided.") }}

        {%- elif not target_relation and default is not none -%}

          {{ log("Relation " ~ table ~ " does not exist. Returning the default value: " ~ default) }}

          {{ return(default) }}

        {%- else -%}

            {# fields_to_ignore will be comma separated array of fields to ignore in returned list, i.e. ['name','email','address'] #}

            {% set field_list = fields_to_ignore %}

            SELECT
                DISTINCT column_name
            FROM `{{ table.database }}.{{ table.schema }}.INFORMATION_SCHEMA.COLUMNS`
            WHERE table_name = "{{ table.identifier }}"
            AND column_name NOT IN (
                {% for field in field_list %}
                    '{{ field }}'
                {% if not loop.last %},{% endif %}
                {% endfor %}
                )

        {% endif %}

    {%- endcall -%}

    {%- set value_list = load_result('get_column_names') -%}

    {%- if value_list and value_list['data'] -%}
        {%- set values = value_list['data'] | map(attribute=0) | list %}
        {{ return(values) }}
    {%- else -%}
        {{ return(default) }}
    {%- endif -%}

{%- endmacro %}


