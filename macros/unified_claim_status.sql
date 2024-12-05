{% macro unified_claim_status(claim_status, claim_resolution_status) %}
    CASE 
        WHEN {{ claim_status }} = 'Denied' THEN 'Rejected'
        WHEN {{ claim_status }} = 'Approved' THEN 'Settled'
        ELSE {{ claim_resolution_status }}
    END
{% endmacro %}
