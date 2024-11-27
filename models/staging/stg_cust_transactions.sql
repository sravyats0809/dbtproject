{{ config(
    materialized="table"
) }}

SELECT 
TransactionID,
CustomerID,
TransactionDate,
TransactionAmount,
TransactionType,
PaymentMethod,
TransactionStatus
FROM
{{ref('raw_customer_transactions')}}