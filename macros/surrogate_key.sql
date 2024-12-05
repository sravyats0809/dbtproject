--macro to generate surrogate key on two columns

{% macro generate_surrogate_key(policy_id, customer_id) %}
md5(CONCAT({{ policy_id }}, {{ customer_id }}))
{% endmacro %}
