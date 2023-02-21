{% macro get_product_active_dats(product_name) %}

{# The default date ranges for defining active will be taken unless the product is listed here #}
{# Default:
    active <= 30 days
    inactive between 31 and 180 days
    dormant > 180 days
 #}

 {% set custom_product_active_days = {
    "some_product" : 
        {"active_max": "100"
        ,"inactive_max": "365"
        }
    , 
    "some_other_product" : 
        {"active_max": "3"
        ,"inactive_max": "20"
        }
} -%}

{% if product_name in custom_product_active_days %}
    {% set relevant_product_days = custom_product_active_days[product_name] %}
    {% set active_max_days = relevant_product_days['active_max'] %}
    {% set inactive_max_days = relevant_product_days['inactive_max'] %}
{% else %}
    {% set active_max_days = 30 %}
    {% set inactive_max_days = 180 %}
{% endif %}


{% set product_days_data = dict() %}
{% do product_days_data.update({'active_max_days': active_max_days}) %}
{% do product_days_data.update({'inactive_max_days': inactive_max_days}) %}
{{ return(product_days_data) }}
{% endmacro %}
