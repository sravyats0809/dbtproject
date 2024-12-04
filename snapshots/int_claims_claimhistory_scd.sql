{% snapshot int_claims_claimhistory_scd %} 

{{
    config(
        target_schema='DBT_ST', 
        target_database='INSUREDEV',
        unique_key='CLAIM_SURROGATE_KEY',
        strategy='check',
        check_cols= 'all'
    )
}}

SELECT * FROM {{ref("int_claims_claimhistory")}}

{% endsnapshot %}