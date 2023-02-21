{% macro union_product_stages(tables, granularity) %}

{#-
    WHAT THIS MACRO DOES:
    This unions together all the individual lifecycle stage logs of a certain product 

    HOW TO USE:
    Call this macro as part of creating a product specific lifecycle events table

    VARS:
    - tables [ARRAY] - list of all the tables to union together
    - granularity [STRING] - accepted values:
                                        - customer
                                        - location

 -#}

WITH source_data AS (
    {{  union_versions(
        source_table_names=tables
        )
    }}
)

SELECT
    source_data.*
    {# the next_timestamp is useful for working out when that current row is valid until. Most useful for eligible/enabled #}
    , LEAD(source_data.timestamp) OVER(PARTITION BY source_data.product_entity_id, source_data.event_category ORDER BY source_data.timestamp ASC) AS next_timestamp
FROM source_data

{% endmacro %}
