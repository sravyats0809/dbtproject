{{ config(
    materialized="ephemeral"
) }}



select
    c.customerid,
    c.customername,
    c.customerage,
    c.gender,
    c.maritalstatus,
    c.address,
    c.city,
    c.state,
    c.postalcode,
    c.phonenumber,
    c.email,
    c.occupation,
    c.annualincome,
    c.dateofbirth,
    c.registrationdate,
    t.transactionid,
    t.transactiondate,
    t.transactionamount,
    t.transactiontype,
    t.paymentmethod,
    t.transactionstatus
from {{ ref('stg_customer_data') }} c
left join {{ ref('stg_cust_transactions') }} t
    on c.customerid = t.customerid
