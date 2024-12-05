{{ config(
    materialized="table"
) }}

SELECT 
ClaimID,
CustomerID,
PolicyID,
ClaimType,
ClaimAmount,
ClaimDate,
ClaimStatus,
ClaimReason,
ClaimPaidAmount,
ClaimDeniedAmount,
ClaimFiledBy,
ClaimAdjuster,
ClaimResolutionDate,
ClaimResolutionStatus,
ClaimInvestigation,
from {{ ref('raw_claim_history') }}
