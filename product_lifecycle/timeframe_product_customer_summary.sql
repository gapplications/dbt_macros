{% macro timeframe_product_customer_summary(time_granularity, product_lifecycle_tables, granularity) %}

{#-
    WHAT THIS MACRO DOES:
    This macro takes all the individual product lifecycle tables, and creates a flattened summary table at the given granularity

    HOW TO USE:
    Call this macro for creating a summary over time of the different statuses of products

    VARS:
    - time_granularity [STRING] - the time granularity of the table. accepted values:
                                                                                    - day
                                                                                    - week
                                                                                    - month
    - product_lifecycle_tables [ARRAY] - list of all individual product lifecycle tables
    - granularity [STRING] - accepted values:
                                        - customer
                                        - location

    RESULTING SCHEMA:
    week/month, product_x_eligible_count, product_x_aware_count, product_x_active_count, etc.

 -#}

{# if we are dealing with weeks we want to use isoweek format #}
{% set iso_string = '' %}
{% if time_granularity == 'week' %}
    {% set iso_string = 'iso' %}
{% endif %}
{% set date_part_string = iso_string ~ time_granularity %}

{# create a date spine of whatever granularity is given #}

{% if time_granularity == 'day' %}
    {% set spine_start = 'date_sub(current_date(), interval 90 day)' %}
{% elif time_granularity == 'week' %}
    {% set spine_start = 'date_sub(current_date(), interval 70 week)' %}
{% elif time_granularity == 'month' %}
    {% set spine_start = 'date_sub(current_date(), interval 18 month)' %}
{% endif %}

{%- set start_date_string = "date_trunc(" ~ spine_start ~", " ~ date_part_string ~ ")" -%}
{%- set end_date_string = "date_sub(date_add(current_date(),interval 1 " ~ time_granularity ~ "), interval 1 day)" -%}
 WITH date_spine AS (
     {{ dbt_utils.date_spine(
        datepart = time_granularity,
        start_date = start_date_string,
        end_date = end_date_string
        ) 
    }}
 )

{# Get list of unique ids across tables which will be used as base for later joins to #}
, all_unique_ids AS (
    {% for product_lifecycle_table in product_lifecycle_tables %}
    SELECT DISTINCT
        {{ granularity }}_id
    FROM {{ product_lifecycle_table }}
    {% if not loop.last %}
    UNION DISTINCT
    {% endif %}
    {% endfor %}
)

, all_{{ granularity }}s_by_{{ time_granularity }} AS (
    SELECT 
        all_unique_ids.{{ granularity }}_id
        , date_spine.date_{{time_granularity}}
    FROM all_unique_ids
        CROSS JOIN date_spine
)

{# Create a list of all cte names - will be easier to reference in later joins #}
{%- set cte_list = [] -%}
{% for product_lifecycle_table in product_lifecycle_tables %}

    {# Get all the unique categories in the source lifecycle table #}
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

    {# Create CTE for each category in each product #}
    {% for category in all_categories %}

        {%- set cte_name = '_' ~ product_lifecycle_table.identifier ~ '_' ~ category -%}
        {%- set _ = cte_list.append(cte_name) -%}
        , {{ cte_name }} AS (

            SELECT 
                {# On the date granularity given get an aggregated count of all customers in that category #}
                all_{{ granularity }}s_by_{{ time_granularity }}.{{ granularity }}_id
                , all_{{ granularity }}s_by_{{ time_granularity }}.date_{{time_granularity}}
                , COUNT(DISTINCT lc.product_entity_id) AS {{ product_name }}_is_{{ category }}_count

                {# If dealing with the active category also aggregate the count and amount measures #}
                {% if category == 'active' %}
                , SUM(lc.count) AS {{ product_name }}_total_active_count
                , SUM(lc.amount) AS {{ product_name }}_total_active_amount
                {% endif %}

            FROM all_{{ granularity }}s_by_{{ time_granularity }}
                LEFT JOIN {{ product_lifecycle_table }} lc
                    {# For active we want to look at active in the period, so adjust the join #}
                    {% if category == 'active' %}
                    ON DATE_TRUNC(DATE(all_{{ granularity }}s_by_{{ time_granularity }}.date_{{ time_granularity }}), {{ time_granularity }}) = DATE_TRUNC(DATE(lc.timestamp), {{ time_granularity }})
                    {% else %}
                    {# Otherwise look at everything up to and including that date #}
                    ON TIMESTAMP(all_{{ granularity }}s_by_{{ time_granularity }}.date_{{ time_granularity }}) BETWEEN TIMESTAMP_TRUNC(lc.timestamp, DAY) AND COALESCE(lc.next_timestamp, '2999-12-31 23:59:59')
                    {% endif %}
                    AND all_{{ granularity }}s_by_{{ time_granularity }}.{{ granularity }}_id = lc.{{ granularity }}_id
            WHERE lc.event_category = '{{ category }}'
                {% if category == 'enabled' %}
                AND lc.event = '{{ category }}'
                {% endif %}
            GROUP BY 1,2
        {% if category == 'active' %}
        {# If the category is active, we also want to pull some inactive/dormant numbers #}
        )
        {# Need to create a list of users to exclude from inactive date range to avoid subquery #}
        {# This looks at everyone who has been active in the last x days, 30 by default #}
        , {{ product_name }}_inactive_exclusions AS (
            SELECT
                DISTINCT 
                all_{{ granularity }}s_by_{{ time_granularity }}.{{ granularity }}_id
                , all_{{ granularity }}s_by_{{ time_granularity }}.date_{{time_granularity}}
            FROM all_{{ granularity }}s_by_{{ time_granularity }}
                LEFT JOIN {{ product_lifecycle_table }} lc
                    ON DATE(lc.timestamp) BETWEEN DATE_SUB(all_{{ granularity }}s_by_{{ time_granularity }}.date_{{time_granularity}}, INTERVAL {{ product_days_data['active_max_days'] }} DAY) AND
                                        all_{{ granularity }}s_by_{{ time_granularity }}.date_{{time_granularity}}
                    AND all_{{ granularity }}s_by_{{ time_granularity }}.{{ granularity }}_id = lc.{{ granularity }}_id
            WHERE lc.event_category = '{{ category }}'
                AND lc.count >= 1
        )
        {%- set cte_name = '_' ~ product_lifecycle_table.identifier ~ '_inactive'-%}
        {%- set _ = cte_list.append(cte_name) -%}
        , {{ cte_name }} AS (
            {# CTE for usage in the inactive window of x-y days ago, 30-180 by default #}
            SELECT 
                all_{{ granularity }}s_by_{{ time_granularity }}.{{ granularity }}_id
                , all_{{ granularity }}s_by_{{ time_granularity }}.date_{{time_granularity}}
                , COUNT(DISTINCT CASE WHEN lc_inactive.count >= 1 THEN lc_inactive.product_entity_id END) AS {{ product_name }}_is_inactive_count

            FROM all_{{ granularity }}s_by_{{ time_granularity }}
                LEFT JOIN {{ product_lifecycle_table }} lc_inactive
                    ON DATE(lc_inactive.timestamp) BETWEEN 
                        DATE_SUB(all_{{ granularity }}s_by_{{ time_granularity }}.date_{{ time_granularity }}, INTERVAL {{ product_days_data['inactive_max_days'] }} DAY) AND DATE_SUB(all_{{ granularity }}s_by_{{ time_granularity }}.date_{{ time_granularity }}, INTERVAL {{ product_days_data['active_max_days'] }} DAY)
                    AND all_{{ granularity }}s_by_{{ time_granularity }}.{{ granularity }}_id = lc_inactive.{{ granularity }}_id
                    AND lc_inactive.event_category = '{{ category }}'
                LEFT JOIN {{ product_name }}_inactive_exclusions
                    ON lc_inactive.{{ granularity }}_id = {{ product_name }}_inactive_exclusions.{{ granularity }}_id
                    AND all_{{ granularity }}s_by_{{ time_granularity }}.date_{{time_granularity}} = {{ product_name }}_inactive_exclusions.date_{{time_granularity}}
            {# Null join will ensure no user who has used the product in the last 30 days will be included in the inactive count #}
            WHERE {{ product_name }}_inactive_exclusions.{{ granularity }}_id IS NULL
            GROUP BY 1,2
        )
        {# Same logic for inactive applied for domant user count but this is > 180 days on inactivity #}
        , {{ product_name }}_dormant_exclusions AS (
            SELECT
                DISTINCT 
                all_{{ granularity }}s_by_{{ time_granularity }}.{{ granularity }}_id
                , all_{{ granularity }}s_by_{{ time_granularity }}.date_{{time_granularity}}
            FROM all_{{ granularity }}s_by_{{ time_granularity }}
                LEFT JOIN {{ product_lifecycle_table }} lc
                    ON DATE(lc.timestamp) BETWEEN DATE_SUB(all_{{ granularity }}s_by_{{ time_granularity }}.date_{{time_granularity}}, INTERVAL {{ product_days_data['inactive_max_days'] }} DAY) AND
                                        all_{{ granularity }}s_by_{{ time_granularity }}.date_{{time_granularity}}
                    AND all_{{ granularity }}s_by_{{ time_granularity }}.{{ granularity }}_id = lc.{{ granularity }}_id
            WHERE lc.event_category = '{{ category }}'
                AND lc.count >= 1
        )
        {%- set cte_name = '_' ~ product_lifecycle_table.identifier ~ '_dormant'-%}
        {%- set _ = cte_list.append(cte_name) -%}
        , {{ cte_name }} AS (

            SELECT 
                all_{{ granularity }}s_by_{{ time_granularity }}.{{ granularity }}_id
                , all_{{ granularity }}s_by_{{ time_granularity }}.date_{{time_granularity}}
                , COUNT(DISTINCT CASE WHEN lc_dormant.count >= 1 THEN lc_dormant.product_entity_id END) AS {{ product_name }}_is_dormant_count

            FROM all_{{ granularity }}s_by_{{ time_granularity }}
                LEFT JOIN {{ product_lifecycle_table }} lc_dormant
                    ON DATE(lc_dormant.timestamp) <=  DATE_SUB(all_{{ granularity }}s_by_{{ time_granularity }}.date_{{ time_granularity }}, INTERVAL {{ product_days_data['inactive_max_days'] }} DAY)
                    AND all_{{ granularity }}s_by_{{ time_granularity }}.{{ granularity }}_id = lc_dormant.{{ granularity }}_id
                    AND lc_dormant.event_category = '{{ category }}'
                LEFT JOIN {{ product_name }}_dormant_exclusions
                    ON lc_dormant.{{ granularity }}_id = {{ product_name }}_dormant_exclusions.{{ granularity }}_id
                    AND all_{{ granularity }}s_by_{{ time_granularity }}.date_{{time_granularity}} = {{ product_name }}_dormant_exclusions.date_{{time_granularity}}
            WHERE {{ product_name }}_dormant_exclusions.{{ granularity }}_id IS NULL
            GROUP BY 1,2
        {% endif %}
        )
    {% endfor %}
{% endfor %}


SELECT
    all_{{ granularity }}s_by_{{ time_granularity }}.{{ granularity }}_id
    , all_{{ granularity }}s_by_{{ time_granularity }}.date_{{time_granularity}}
    {% for cte in cte_list %}
    , {{ cte }}.* EXCEPT( {{ granularity }}_id, date_{{ time_granularity }})
    {% endfor %}

FROM all_{{ granularity }}s_by_{{ time_granularity }}
    {% for cte in cte_list %}
    LEFT JOIN {{ cte }}
        ON all_{{ granularity }}s_by_{{ time_granularity }}.date_{{ time_granularity }} = {{ cte }}.date_{{ time_granularity }}
            AND all_{{ granularity }}s_by_{{ time_granularity }}.{{ granularity }}_id = {{ cte }}.{{ granularity }}_id
    {% endfor %}

ORDER BY 2 ASC, 1 ASC
{% endmacro %}
