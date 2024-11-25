{{ config(
    materialized="table"
) }}

SELECT 
PaymentID,
PolicyID,
CustomerID,
PaymentAmount,
PaymentDate,
PaymentMethod,
PaymentStatus,
PaymentFrequency,
LateFee,
PaymentProcessor,
PaymentReferenceNumber,
TransactionID,
DiscountApplied,
RefundAmount,
PaymentSource,

from {{ ref('raw_premium_payment') }}