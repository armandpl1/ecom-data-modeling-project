-- Silver: SCD Type 2 history of customers. One row per version.
{{ scd2_from_history(
    relation=ref('stg_customers'),
    unique_key='customer_id',
    surrogate_key='customer_sk',
    tracked_columns=[
        'first_name', 'last_name', 'email', 'phone_number', 'phone_extension',
        'street_address', 'city', 'state', 'postal_code', 'country',
        'customer_segment', 'marketing_opt_in'
    ]
) }}
