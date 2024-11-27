{{ config(
    materialized="incremental",
    unique_key="CLAIM_SURROGATE_KEY",
    merge_exclude_columns = ['created_at']
) }}
SELECT 
    -- Generate surrogate key
    md5(CONCAT(p.POLICYID, p.CUSTOMERID)) AS CLAIM_SURROGATE_KEY,
    -- Base policy details
    p.POLICYID,
    p.CUSTOMERID,
    p.POLICYTYPE,
    
    -- Transform policy start date
    CASE 
        WHEN p.POLICYSTARTDATE IS NULL THEN CURRENT_DATE
        ELSE p.POLICYSTARTDATE
    END AS POLICYSTARTDATE,  -- Default to current date if NULL

    --Tranform policy end date for 1 year if null
    CASE 
        WHEN p.POLICYENDDATE IS NULL THEN DATEADD(YEAR, 1, CURRENT_DATE)
        ELSE p.POLICYENDDATE
    END AS POLICYENDDATE,  -- Default to 1 year from now if NULL
    
    -- Transform policy premium (use IFNULL for handling NULLs in premium amount/ I can also use coalesce function)
    IFNULL(p.POLICYPREMIUM, 0) AS POLICYPREMIUM,  -- Replace NULL with 0
    
    -- Clean up coverage amount (using ROUND to avoid excessive decimal points)
    ROUND(p.POLICYCOVERAGEAMOUNT, 2) AS POLICYCOVERAGEAMOUNT,  -- Round coverage to 2 decimal places
    
    -- Policy status (trim spaces and convert to upper case)
    UPPER(TRIM(p.POLICYSTATUS)) AS POLICYSTATUS,  -- Clean and standardize status text
    
    p.POLICYHOLDERNAME,
    p.BENEFICIARYNAME,
    p.POLICYAGENT,
    
    -- Transform renewal date (if NULL, default to policy end date)
    COALESCE(p.RENEWALDATE, p.POLICYENDDATE) AS RENEWALDATE,
    
    p.PAYMENTFREQUENCY,
    p.DISCOUNT,
    p.UNDERWRITINGCOMPANY,

    -- Policy payment details with more transformations
    CASE
        WHEN pp.PAYMENTAMOUNT IS NULL THEN 0  -- Replace null payment amounts with 0
        ELSE pp.PAYMENTAMOUNT
    END AS PAYMENTAMOUNT,
    
    -- Handle payment date (if NULL, use policy start date)
    COALESCE(pp.PAYMENTDATE, p.POLICYSTARTDATE) AS PAYMENTDATE,
    
    -- Handle case when payment method is missing (set a default value)
    IFNULL(pp.PAYMENTMETHOD, 'UNKNOWN') AS PAYMENTMETHOD,
    
    -- Handle late fee (replace null with 0)
    IFNULL(pp.LATEFEE, 0) AS LATEFEE,
    
    -- Discount applied (transform to boolean-like value, if 0, consider as 'NO')
    CASE
        WHEN pp.DISCOUNTAPPLIED = 0 THEN 'NO'
        WHEN pp.DISCOUNTAPPLIED > 0 THEN 'YES'
        ELSE 'UNKNOWN'
    END AS DISCOUNTAPPLIED,
    
    -- Refund amount (set to 0 if NULL)
    IFNULL(pp.REFUNDAMOUNT, 0) AS REFUNDAMOUNT,

    -- Add `created_at` and `updated_at` columns
    CURRENT_TIMESTAMP AS created_at,  -- Current timestamp for creation
    CURRENT_TIMESTAMP AS updated_at   -- Current timestamp for update

FROM
    {{ ref('stg_policy') }} p
FULL OUTER JOIN
    {{ ref('stg_premium_payment') }} pp
    ON p.POLICYID = pp.POLICYID
    AND p.CUSTOMERID = pp.CUSTOMERID


{% if is_incremental() %}

    -- In incremental mode, only include new or updated records
    
WHERE CLAIM_SURROGATE_KEY NOT IN (SELECT CLAIM_SURROGATE_KEY FROM {{ this }})

{% endif %}



       -- getdate() as created_dtm,
       -- getdate() as updated_dtm -- Using CURRENT_TIMESTAMP() for both full and incremental loads
    

    

