{% macro flatten_product_lifecycle(product_lifecycle_tables, granularity) %}

{#-
    WHAT THIS MACRO DOES:
    This macro takes all the individual product lifecycle tables, and creates a flattened summary table at the given granularity

    HOW TO USE:
    Call this macro as part of creating a product specific lifecycle events table

    VARS:
    - product_lifecycle_tables [ARRAY] - list of all individual product lifecycle tables
    - granularity [STRING] - accepted values:
                                        - customer
                                        - location

    RESULTING SCHEMA:
    customer_id/location_id, product_x_first_eligible, product_x_last_eligible, product_x_first_aware, etc.

 -#}

    {# Get list of unique ids across tables which will be used as base for later joins to #}
    WITH all_unique_ids AS (
        {% for product_lifecycle_table in product_lifecycle_tables %}
        SELECT DISTINCT
            {{ granularity }}_id
        FROM {{ product_lifecycle_table }}
        {% if not loop.last %}
        UNION DISTINCT
        {% endif %}
        {% endfor %}
    )

    {% for product_lifecycle_table in product_lifecycle_tables %}

    , _{{ product_lifecycle_table.identifier }} AS (

        {# need to know what categories are included in each product table #}
        {%- call statement('get_product_and_categories', fetch_result=true) %}

            SELECT
                DISTINCT
                product
                , event_category
            FROM {{ product_lifecycle_table }}

        {%- endcall -%}

        {%- set value_list = load_result('get_product_and_categories') -%}
        {%- set all_categories = [] -%}
        {%- set product_name = value_list['data'] | first | first -%}
        {%- set product_days_data = get_product_active_dats(product_name) -%}
        {% for x in value_list['data'] %}
            {%- set _ = all_categories.append(x[1]) -%}
        {% endfor %}

        SELECT 
            {{granularity}}_id
            , STRUCT(
            {# if eligible is a category in source table then add the eligible flags #}
            {% if 'eligible' in all_categories %}
            STRUCT(
                MIN(
                    CASE
                        WHEN event_category = 'eligible'
                        AND event = 'eligible'
                        THEN timestamp 
                    END
            ) AS {{ product_name }}_first_eligible
            ,  MAX(
                CASE
                    WHEN event_category = 'eligible'
                    AND event = 'eligible'
                    THEN timestamp 
                END
            ) AS {{ product_name }}_last_eligible
            ,  MAX(
                CASE
                    WHEN event_category = 'eligible'
                    AND event = 'not_eligible'
                    THEN timestamp 
                END
            ) AS {{ product_name }}_last_not_eligible
            ,  COUNT(DISTINCT 
                CASE
                    WHEN event_category = 'eligible'
                    AND event = 'eligible'
                    THEN event_id 
                END
            ) AS {{ product_name }}_eligible_count
            ,  CASE
                WHEN 
                    MAX(
                        CASE
                            WHEN event_category = 'eligible'
                            AND event = 'eligible'
                            THEN timestamp 
                        END
                    )
                    >
                    COALESCE(
                        MAX(
                            CASE
                                WHEN event_category = 'eligible'
                                AND event = 'not_eligible'
                                THEN timestamp 
                            END
                        )
                    , TIMESTAMP('2000-01-01')
                    )
                THEN TRUE
                ELSE FALSE
            END AS {{ product_name }}_is_eligible
            ) AS eligible
            {% endif %}


            {# if aware is a category in source table then add the awareness flags #}
            {% if 'aware' in all_categories %}
            {% if 'eligible' in all_categories %}
            , 
            {% endif %}
            STRUCT(
            MIN(
                CASE
                    WHEN event_category = 'aware'
                    THEN timestamp 
                END
            ) AS {{ product_name }}_first_aware
            ,  MAX(
                CASE
                    WHEN event_category = 'aware'
                    THEN timestamp 
                END
            ) AS {{ product_name }}_last_aware
            ,  COUNT(DISTINCT 
                CASE
                    WHEN event_category = 'aware'
                    THEN event_id 
                END
            ) AS {{ product_name }}_aware_count
            ) AS aware
            {% endif %}


            {# if onboarding is a category in source table then add the onboarding flags #}
            {% if 'onboarding' in all_categories %}
            {% if 'eligible' in all_categories or 'aware' in all_categories %}
            , 
            {% endif %}
            STRUCT(
            MIN(
                CASE
                    WHEN event_category = 'onboarding'
                    AND event = 'applied'
                    THEN timestamp 
                END
            ) AS {{ product_name }}_first_applied
            ,  MAX(
                CASE
                    WHEN event_category = 'onboarding'
                    AND event = 'applied'
                    THEN timestamp 
                END
            ) AS {{ product_name }}_last_applied
            ,  COUNT(DISTINCT
                CASE
                    WHEN event_category = 'onboarding'
                    AND event = 'applied'
                    THEN event_id 
                END
            ) AS {{ product_name }}_applied_count
            , MIN(
                CASE
                    WHEN event_category = 'onboarding'
                    AND event = 'declined'
                    THEN timestamp 
                END
            ) AS {{ product_name }}_first_declined
            ,  MAX(
                CASE
                    WHEN event_category = 'onboarding'
                    AND event = 'declined'
                    THEN timestamp 
                END
            ) AS {{ product_name }}_last_declined
            ,  COUNT(DISTINCT
                CASE
                    WHEN event_category = 'onboarding'
                    AND event = 'declined'
                    THEN event_id 
                END
            ) AS {{ product_name }}_declined_count
            ) AS onboarding
            {% endif %}


            {# if enabled is a category in source table then add the enabled flags #}
            {% if 'enabled' in all_categories %}
            {% if 'eligible' in all_categories or 'aware' in all_categories or 'onboarding' in all_categories %}
            , 
            {% endif %}
            STRUCT(
            MIN(
                CASE
                    WHEN event_category = 'enabled'
                        AND event = 'enabled'
                    THEN timestamp 
                END
            ) AS {{ product_name }}_first_enabled
            ,  MAX(
                CASE
                    WHEN event_category = 'enabled'
                        AND event = 'enabled'
                    THEN timestamp 
                END
            ) AS {{ product_name }}_last_enabled
            ,MIN(
                CASE
                    WHEN event_category = 'enabled'
                        AND event = 'disabled'
                    THEN timestamp 
                END
            ) AS {{ product_name }}_first_disabled
            ,  MAX(
                CASE
                    WHEN event_category = 'enabled'
                        AND event = 'disabled'
                    THEN timestamp 
                END
            ) AS {{ product_name }}_last_disabled
            ) AS enabled
            {% endif %}


            {# if active is a category in source table then add the activity flags #}
            {% if 'active' in all_categories %}
            {% if 'eligible' in all_categories or 'aware' in all_categories or 'onboarding' in all_categories or 'enabled' in all_categories %}
            , 
            {% endif %}
            STRUCT(
            MIN(
                CASE
                    WHEN event_category = 'active'
                    THEN timestamp 
                END
            ) AS {{ product_name }}_first_active
            ,  MAX(
                CASE
                    WHEN event_category = 'active'
                    THEN timestamp 
                END
            ) AS {{ product_name }}_last_active
            , CASE
                WHEN 
                    MAX(
                        CASE
                            WHEN event_category = 'active'
                            THEN timestamp 
                        END
                    ) >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {{ product_days_data['active_max_days'] }} DAY)
                THEN TRUE
                ELSE FALSE
            END AS {{ product_name }}_is_active
            , CASE
                WHEN 
                    MAX(
                        CASE
                            WHEN event_category = 'active'
                            THEN timestamp 
                        END
                    ) BETWEEN TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {{ product_days_data['inactive_max_days'] }} DAY) AND TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {{ product_days_data['active_max_days'] }} DAY)
                THEN TRUE
                ELSE FALSE
            END AS {{ product_name }}_is_inactive
            , CASE
                WHEN 
                    MAX(
                        CASE
                            WHEN event_category = 'active'
                            THEN timestamp 
                        END
                    ) < TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {{ product_days_data['inactive_max_days'] }} DAY)
                THEN TRUE
                ELSE FALSE
            END AS {{ product_name }}_is_dormant
            ,  SUM(
                CASE
                    WHEN event_category = 'active'
                    THEN count 
                END
            ) AS {{ product_name }}_active_count
            ,  SUM(
                CASE
                    WHEN event_category = 'active'
                    THEN amount 
                END
            ) AS {{ product_name }}_active_amount
            ) AS active
            {% endif %}
            ) AS {{ product_name }}
        FROM {{ product_lifecycle_table }}
        GROUP BY 1
    )
    {% endfor %}


    , all_product_flags AS ( 

        SELECT
            all_unique_ids.{{granularity}}_id

            {% for product_lifecycle_table in product_lifecycle_tables %}
            , _{{ product_lifecycle_table.identifier }}.* EXCEPT({{granularity}}_id)
            {% endfor %}

        FROM all_unique_ids

            {% for product_lifecycle_table in product_lifecycle_tables %}
            LEFT JOIN _{{ product_lifecycle_table.identifier }} 
                ON all_unique_ids.{{granularity}}_id = _{{ product_lifecycle_table.identifier }}.{{granularity}}_id
            {% endfor %}
        WHERE all_unique_ids.{{granularity}}_id IS NOT NULL
    )

    SELECT *
    FROM all_product_flags

{% endmacro %}
