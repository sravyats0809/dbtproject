{{ config(
    materialized="table"
) }}

SELECT 
    -- Generate surrogate key
    md5(CONCAT(sc.CLAIMID, sc.CUSTOMERID, sc.POLICYID)) AS CLAIM_SURROGATE_KEY,
    -- Base claim details from stg_claim
    sc.CLAIMID,
    sc.CUSTOMERID,
    sc.POLICYID,
    COALESCE(sc.CLAIMTYPE, sch.CLAIMTYPE) AS CLAIMTYPE,
    COALESCE(sc.CLAIMAMOUNT, sch.CLAIMAMOUNT) AS CLAIMAMOUNT,
    COALESCE(sc.CLAIMDATE, sch.CLAIMDATE) AS CLAIMDATE,
    COALESCE(sc.CLAIMSTATUS, sch.CLAIMSTATUS) AS CLAIMSTATUS,
    
    -- Unified claim resolution details
    CASE 
        WHEN sc.CLAIMSTATUS = 'Denied' THEN 'Rejected'
        WHEN sc.CLAIMSTATUS = 'Approved' THEN 'Settled'
        ELSE sch.CLAIMRESOLUTIONSTATUS
    END AS CLAIMRESOLUTIONSTATUS,
    
    COALESCE(sc.CLAIMRESOLUTIONDATE, sch.CLAIMRESOLUTIONDATE) AS CLAIMRESOLUTIONDATE,
    COALESCE(sc.CLAIMAPPROVALAMOUNT, sch.CLAIMPAIDAMOUNT) AS CLAIMAPPROVALAMOUNT,
    COALESCE(sc.CLAIMCAUSE, sch.CLAIMREASON) AS CLAIMCAUSE,
    
    -- Claim payment details
    CASE 
        WHEN sc.CLAIMSTATUS = 'Denied' THEN COALESCE(sch.CLAIMDENIEDAMOUNT, 0)
        ELSE COALESCE(sc.CLAIMPAYMENTAMOUNT, 0)
    END AS CLAIMPAYMENTAMOUNT,
    
    CASE 
        WHEN sc.CLAIMSTATUS = 'Denied' THEN NULL
        ELSE COALESCE(sc.CLAIMPAYMENTDATE, sch.CLAIMRESOLUTIONDATE)
    END AS CLAIMPAYMENTDATE,
    
    -- Claim adjuster and reporting details
    COALESCE(sc.CLAIMADJUSTER, sch.CLAIMADJUSTER) AS CLAIMADJUSTER,
    COALESCE(sc.CLAIMREPORTEDBY, sch.CLAIMFILEDBY) AS CLAIMREPORTEDBY,
    
    -- Claim description or investigation status
    COALESCE(sc.CLAIMDESCRIPTION, sch.CLAIMINVESTIGATION) AS CLAIMDESCRIPTION
    
FROM 
    {{ ref('stg_claim') }} sc
LEFT JOIN 
    {{ ref('stg_claim_history') }} sch
ON 
    sc.CLAIMID = sch.CLAIMID
    ORDER BY sc.CLAIMID
