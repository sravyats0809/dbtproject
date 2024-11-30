-- tests/non_negative_claim_payment_amount.sql


SELECT
    CLAIMID,  -- You can select any other relevant fields if necessary for debugging
    CLAIMPAYMENTAMOUNT
FROM
    {{ ref('int_claims_claimhistory') }}  -- Replace 'your_model_name' with your actual model name
WHERE
    CLAIMPAYMENTAMOUNT < 0

