--This test should fail only when policytype contain letters other than A-z
SELECT 
POLICYID,
CUSTOMERID, 
POLICYTYPE
FROM {{ ref('int_policy_payment')}}
WHERE POLICYTYPE NOT REGEXP '^[A-Za-z]+$'
