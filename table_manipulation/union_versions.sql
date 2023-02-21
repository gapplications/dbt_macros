{% macro union_versions(source_table_names, to_snake=False, apply_limit_data_in_ci=False, incremental_where_clause='1=1')  %}

{#- 
    WHAT THIS MACRO DOES:
    This macro looks to union different versions of a table together to create one complete log 
    It will take all unique columns from every version. If a column does not exist in a version then it will be nulled as the correct data type

    HOW TO USE:
    Call this macro and put all the versions of a table you want to union into an array under the variable source_table_names

    THINGS TO BE WARY OF:
    Sometimes in different versions of the same topic columns could be renamed, i.e. customerRef to CustomerReference
    This macro can not deal with these edge cases.
    In this case it is recommended to use this macro to union everything together as a CTE
    Then in your final statement SELECT * EXCEPT(overlapping_fields) then do a COALESCE(new_field, old_field)

 -#}
{#- Get dictionary of all column names and data types in all tables selected -#}
{%- set all_columns = [] -%}
{%- for source_table in source_table_names -%}
    {% set target_relation = adapter.get_relation(database=source_table.database,
                                          schema=source_table.schema,
                                         identifier=source_table.identifier) %}
    {#- Check source_table exists -#}
    {%- if not target_relation and default is none -%}
      {{ exceptions.raise_compiler_error("In union_versions(): relation " ~ source_table ~ " does not exist and no default value was provided.") }}
    {%- elif not target_relation and default is not none -%}
      {{ log("Relation " ~ source_table ~ " does not exist. Returning the default value: " ~ default) }}
    {%- else -%}
        {%- set table_columns = get_column_names_and_types(table=source_table) -%}
        {#- Append list of all column names and data types into master list -#}
        {%- set _ = all_columns.append(table_columns) -%}
    {%- endif -%}
{%- endfor -%}
{%- set column_list = [] -%}
{#- Get distinct list of all column names and data types in all tables -#}

{% set columns_to_rename = ['_PARTITIONTIME', 'partition_time'] %}

{%- for x in all_columns -%}
    {%- for y in x -%}
        {%- if y not in column_list and y[0] not in columns_to_rename -%}
            {%- do column_list.append(y) -%}
        {%- endif -%}
    {%- endfor -%}
{%- endfor -%}



{#- Loop through all source tables and created unioned sql logic -#}
{%- for source_table in source_table_names -%}
SELECT 
    "{{ source_table.identifier }}" AS source_table_version
    {% set table_columns = get_column_names_and_types(table=source_table) %}
    {#- Loop through master column list -#}
    {%- for column in column_list -%}
    {% if column in table_columns and column[0] not in columns_to_rename %}
    {#- If column is in source table then select it -#}
    , {{ column[0] }} AS {{ camel_to_snake(column[0], to_snake) }}
    {% else %}
    {#- Otherwise cast it as a null -#}
    , CAST(NULL AS {{ column[1] }} ) AS {{ camel_to_snake(column[0], to_snake) }}
    {% endif %}
    {%- endfor -%}
    {%- set table_column_names = get_column_names(source_table, ['']) -%}
    {% for renamed_column in columns_to_rename %}
    {% if renamed_column in table_column_names and renamed_column == '_PARTITIONTIME' %}
    , _PARTITIONTIME AS partition_time
    {% elif renamed_column in table_column_names and renamed_column == 'partition_time' %}
    , partition_time AS partition_time
    {% endif %}
    {% endfor %}

FROM {{ source_table }}
where true
{% if is_incremental() %}
and {{ incremental_where_clause }}
{% endif %}
{% if '_PARTITIONTIME' in table_column_names and apply_limit_data_in_ci %}
and {{ limit_data_in_ci_timestamp('_PARTITIONTIME', 30) }}
{% endif %}

{% if not loop.last %}
UNION ALL
{% endif %}
{%- endfor -%}
{% endmacro %}