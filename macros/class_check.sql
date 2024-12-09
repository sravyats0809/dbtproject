{% macro class_check(table_name, column_name, value_to_check) %}
(
    CASE
        WHEN {{ value_to_check }} IN (SELECT {{ column_name }} FROM {{ table_name }}) THEN 'code1'
        ELSE 'Null'
    END
)
{% endmacro %}
