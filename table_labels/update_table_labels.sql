{% macro update_table_labels(alter_type, project, dataset, table, labels_dict) %}

ALTER {{ alter_type }} `{{ project }}.{{ dataset }}.{{ table }}`
SET OPTIONS (
  labels = [
    {%- for labels in labels_dict -%}
    ('{{ labels[0] }}', '{{ labels[1] }}')
    {% if not loop.last %}
    ,
    {% endif %}
    {%- endfor -%}
    ]);

{% endmacro %}