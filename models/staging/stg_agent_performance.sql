{{ config(
    materialized="table"
) }}

SELECT 
AgentID,
AgentName,
CustomerID,
PolicyID,
ClaimID,
AgentStartDate,
AgentEndDate,
TotalPoliciesManaged,
TotalClaimsHandled,
TotalClaimsPaid,
TotalClaimsDenied,
ClaimsResolutionRate,
AverageClaimAmount,
AgentRegion,
AgentRating,

from {{ ref('raw_agent_performance') }}
