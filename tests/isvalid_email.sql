SELECT 
CUSTOMERID,
EMAIL
FROM 
{{ref("int_customer_downstream")}}
WHERE NOT EMAIL REGEXP '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$'

