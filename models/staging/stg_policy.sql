{{ config(
    materialized="table"
) }}

SELECT 
    PolicyID,
    CustomerID,
    PolicyType,
    PolicyStartDate,
    PolicyEndDate,
    PolicyPremium,
    PolicyCoverageAmount,
    PolicyStatus,
    PolicyHolderName,
    BeneficiaryName,
    PolicyAgent,
    RenewalDate,
    PaymentFrequency,
    Discount,
    UnderwritingCompany
    from {{ ref('raw_policy') }}
