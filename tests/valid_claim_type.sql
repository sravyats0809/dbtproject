-- tests/valid_claim_type.sql

SELECT
    CLAIMID,  -- You can select any other relevant fields for debugging
    CLAIMTYPE
FROM
    {{ ref('int_claims_claimhistory') }}  -- Replace with your actual model name
WHERE
    CLAIMTYPE NOT IN ('Medical', 'Life', 'Auto', 'Home', 'Health')
