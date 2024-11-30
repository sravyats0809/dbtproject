{{ config(
materialized='incremental',
unique_key='POLICY_SURROGATE_KEY'
) }}



WITH joined_data AS (
SELECT
-- Generate surrogate key
md5(CONCAT(p.POLICYID, p.CUSTOMERID)) AS POLICY_SURROGATE_KEY,
p.POLICYID,
p.CUSTOMERID,
p.POLICYTYPE,
p.POLICYSTARTDATE,
p.POLICYENDDATE,
p.POLICYPREMIUM,
pp.PAYMENTID,
pp.PAYMENTAMOUNT,
pp.PAYMENTDATE,
COALESCE({{ this }}.created_at, CURRENT_TIMESTAMP()) AS created_at,
CASE
WHEN {{ this }}.POLICY_SURROGATE_KEY IS NULL
OR {{ this }}.POLICYID != p.POLICYID
OR {{ this }}.CUSTOMERID != p.CUSTOMERID
OR {{ this }}.POLICYTYPE != p.POLICYTYPE
OR {{ this }}.POLICYSTARTDATE != p.POLICYSTARTDATE
OR {{ this }}.POLICYENDDATE != p.POLICYENDDATE
OR {{ this }}.POLICYPREMIUM != p.POLICYPREMIUM
OR {{ this }}.PAYMENTID != pp.PAYMENTID
OR {{ this }}.PAYMENTAMOUNT != pp.PAYMENTAMOUNT
OR {{ this }}.PAYMENTDATE != pp.PAYMENTDATE
THEN CURRENT_TIMESTAMP()
ELSE {{ this }}.updated_at
END AS updated_at

FROM
{{ ref('stg_policy') }} p
FULL OUTER JOIN
{{ ref('stg_premium_payment') }} pp ON p.POLICYID = pp.POLICYID AND p.CUSTOMERID = pp.CUSTOMERID
LEFT JOIN
{{ this }} ON {{ this }}.POLICY_SURROGATE_KEY = md5(CONCAT(p.POLICYID, p.CUSTOMERID))
)
SELECT * FROM joined_data

{% if is_incremental() %}
WHERE POLICY_SURROGATE_KEY NOT IN (SELECT POLICY_SURROGATE_KEY FROM {{ this }})
OR POLICY_SURROGATE_KEY IN (SELECT POLICY_SURROGATE_KEY FROM {{ this }} WHERE updated_at < CURRENT_TIMESTAMP())
{% endif %}