{#
    This macro is run as a post-hook after every model run (dbt run). 
    It will look at the model which has just been run and update the labels of that table in BQ
    The main use case for this is for the labels which get set to flow through to Monte Carlo
    Will only pick up labels for any model which has a commit against it.
#}
{% macro get_table_labels() %}

    {# 
        Firstly we need to return a dataframe of all key pieces of information about the model.
        This takes data from both the github history tables and the model config from dbt.
        You can look through the dbt logs to see what SQL actually gets generated
     #}

    {% call statement('get_all_files', fetch_result=true) %}

    SELECT 
        DISTINCT
        tables.table_catalog AS project
        , tables.table_schema
        , dbt.dbt_table_name AS table_name
        , LOWER(REPLACE(dbt.author, ' ','-')) AS author
        , LOWER(REPLACE(dbt.user_most_commits, ' ','-')) AS user_most_commits
        , LOWER(REPLACE(dbt.user_most_changes, ' ','-')) AS user_most_changes
        , ARRAY_REVERSE(SPLIT('{{ target.project }}','-')) [SAFE_OFFSET(0)] AS environment
        , '{{ model.config.materialized }}' AS materialisation
        , '{{ model.fqn[2]}}' AS folder_level_1
        , {% if model.config.labels is defined -%}
            '{{ model.config.labels.domain }}'
        {%- else -%}
            '{{ project_name }}'
        {%- endif %} AS domain
        {% set model_labels = [] %}
        {% if model.config.labels is defined -%}
            {% for key, value in model.config.labels.items() %}
                {% do model_labels.append(key) %}
            {% endfor %}
        {%- endif %}
        , {% if 'class' in model_labels -%}
            '{{ model.config.labels.class }}'
        {%- else -%}
            {%- if model.fqn[1] == 'output' or model.fqn[1] == 'marts' or model.fqn[1] == 'looker' -%}
                'silver'
            {%- else -%}
                'bronze'
            {%- endif -%}
        {%- endif %} AS class
        , {% if model.config.partition_by is defined -%}
            '{{ model.config.partition_by.field }}'
        {%- else -%}
            ''
        {%- endif %} AS partition_field
        , CASE
            WHEN tables.table_type = 'BASE TABLE'
            THEN 'TABLE'
            ELSE 'VIEW'
        END AS alter_type
    FROM dbt_file_owners dbt
        LEFT JOIN `{{ target.project }}.region-europe-west2.INFORMATION_SCHEMA.TABLES` tables
            ON tables.table_type IN ('BASE TABLE', 'VIEW')
            AND tables.table_schema = '{{ model.schema }}'
            AND tables.table_name = '{{ this.identifier }}'
        LEFT JOIN `{{ target.project }}.region-europe-west2.INFORMATION_SCHEMA.TABLE_OPTIONS` table_options
            ON tables.table_name = table_options.table_name
            AND tables.table_schema = table_options.table_schema
            AND tables.table_catalog = table_options.table_catalog
            AND table_options.option_name = 'labels'

    WHERE tables.table_catalog IS NOT NULL
        AND dbt.dbt_table_name = '{{ model.name }}'
        AND REPLACE(REPLACE(dbt.repository, 'data-dbt-', ''), '-', '_') = '{{ project_name }}'
        

    {% endcall %}

    {% set all_files = load_result('get_all_files') %}

    {#  
        Loop through all returned rows - there should only ever be one returned row
    #}
    {% for file in all_files['data'] %}
        {{ log(file[2]) }}
        {%- if file[1] == model.schema -%}
            {% set alias = model.alias %}

            {# Call another macro to write the ALTER TABLE/VIEW statement with dictionary of all the labels and their values #}
            {% set sql %}
            {{ update_table_labels(file[12], file[0], file[1], alias, [
                                                                ('data_author', file[3])
                                                                ,('data_most_commits', file[4])
                                                                ,('data_most_changes', file[5])
                                                                ,('data_env', file[6])
                                                                ,('data_dbt_repository', project_name)
                                                                ,('data_is_dbt', 'yes')
                                                                ,('data_materialised', file[7])
                                                                ,('data_folder_level_1', file[8])
                                                                ,('domain', file[9])
                                                                ,('class', file[10])
                                                                ,('partition_field', file[11])
                                                                ]) }}
            {% endset %}
            {% do run_query(sql) %}
        {% endif %}
    {% endfor %}
{% endmacro %}