{% snapshot int_customerdata_scd %} 

{{
    config(
        target_schema='DBT_ST', 
        target_database='INSUREDEV',
        unique_key='CUSTOMERID',
        strategy='check',
        check_cols= 'all'
    )
}}

SELECT * FROM {{ref("stg_customer_data")}}

{% endsnapshot %}