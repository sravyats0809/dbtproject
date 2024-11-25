{{ config(
    materialized="table"
) }}

SELECT 
CustomerID,
CustomerName,
CustomerAge,
Gender,
MaritalStatus,
Address,
City,
State,
PostalCode,
PhoneNumber,
Email,
Occupation,
AnnualIncome,
DateOfBirth,
RegistrationDate,
from {{ ref('raw_customer_data') }}
