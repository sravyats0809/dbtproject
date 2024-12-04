{% snapshot int_policy_payment_scd %} #this 

{{
    config(
        target_schema='DBT_ST', 
        target_database='INSUREDEV',
        unique_key='POLICY_SURROGATE_KEY',
        strategy='check',
        check_cols= 'all'
    )
}}

SELECT * FROM {{ref("int_policy_payment")}}

{% endsnapshot %}