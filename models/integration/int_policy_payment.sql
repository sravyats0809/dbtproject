{{ config(
materialized='incremental',
unique_key='POLICY_SURROGATE_KEY',
merge_exclude_columns='created_at'
) }}



SELECT
-- Generate surrogate key
{{ generate_surrogate_key('p.POLICYID', 'p.CUSTOMERID') }} AS POLICY_SURROGATE_KEY,
p.POLICYID,
p.CUSTOMERID,
p.POLICYTYPE,
p.POLICYSTARTDATE,
p.POLICYENDDATE,
p.POLICYPREMIUM,
pp.PAYMENTID,
pp.PAYMENTAMOUNT,
pp.PAYMENTDATE,
CURRENT_TIMESTAMP() AS created_at,
CURRENT_TIMESTAMP() AS updated_at 
FROM
{{ ref('stg_policy') }} p
FULL OUTER JOIN
{{ ref('stg_premium_payment') }} pp ON p.POLICYID = pp.POLICYID AND p.CUSTOMERID = pp.CUSTOMERID


{% if is_incremental() %}
WHERE 
    POLICY_SURROGATE_KEY NOT IN (SELECT POLICY_SURROGATE_KEY FROM {{ this }})
    OR updated_at > (SELECT MAX(updated_at) FROM {{ this }})

{% endif %}