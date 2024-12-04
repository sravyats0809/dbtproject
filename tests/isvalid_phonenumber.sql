SELECT
    CUSTOMERID,
    PHONENUMBER
FROM {{ ref('int_customer_downstream') }}
WHERE NOT PHONENUMBER REGEXP '^[0-9]{3}-[0-9]{3}-[0-9]{4}$'
