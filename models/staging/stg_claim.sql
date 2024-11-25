{{ config(
    materialized="table"
) }}


SELECT 
        ClaimID,
        PolicyID,
        CustomerID,
        ClaimType,
        CAST(ClaimAmount AS FLOAT) AS ClaimAmount,
        CAST(ClaimDate AS DATE) AS ClaimDate,
        ClaimStatus,
        CAST(ClaimApprovalAmount AS FLOAT) AS ClaimApprovalAmount,
        CAST(ClaimResolutionDate AS DATE) AS ClaimResolutionDate,
        ClaimAdjuster,
        ClaimReportedBy,
        ClaimCause,
        ClaimDescription,
        CAST(ClaimPaymentAmount AS FLOAT) AS ClaimPaymentAmount,
        CAST(ClaimPaymentDate AS DATE) AS ClaimPaymentDate,
        from {{ ref('raw_claim') }}


