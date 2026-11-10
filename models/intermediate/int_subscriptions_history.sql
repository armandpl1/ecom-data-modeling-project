-- Silver: SCD Type 2 history of subscriptions. One row per version.
{{ scd2_from_history(
    relation=ref('stg_subscriptions'),
    unique_key='subscription_id',
    surrogate_key='subscription_version_sk',
    tracked_columns=[
        'customer_id', 'plan_name', 'billing_period', 'mrr', 'subscription_status',
        'started_on', 'trial_ends_on', 'cancelled_at'
    ]
) }}
