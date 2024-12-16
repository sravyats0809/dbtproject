{{ config(
    materialized="table"
) }}

SELECT * FROM
{{ref("int_claims_claimhistory_scd")}}