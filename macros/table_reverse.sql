{% macro table_reverse(table_name, column_name) %}
(
    SELECT 
        LISTAGG({{ column_name }}, ',') AS code_list
    FROM {{ table_name }}
)
{% endmacro %}
