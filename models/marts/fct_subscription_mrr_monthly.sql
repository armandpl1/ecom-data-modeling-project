-- Gold: monthly MRR snapshot. One row per subscription per month,
-- describing the subscription as it stood at the end of that month.
-- Only months in which the subscription existed are included.

with subscription_versions as (

    select * from {{ ref('int_subscriptions_history') }}

),

months as (

    select distinct month_start, month_end
    from {{ ref('dim_date') }}
    -- Stop at the last month that has data
    where month_start <= (select max(updated_at) from subscription_versions)

),

state_at_month_end as (

    select
        months.month_start,
        months.month_end,
        subscription_versions.*
    from months
    inner join subscription_versions
        -- version that was valid at the end of the month
        on  subscription_versions.valid_from < months.month_end + interval 1 day
        and (subscription_versions.valid_to >= months.month_end + interval 1 day
             or subscription_versions.valid_to is null)

),

customers as (

    select * from {{ ref('int_customers_history') }}
    where not is_deleted

)

select
    md5(state_at_month_end.subscription_id || '|' || cast(state_at_month_end.month_start as varchar))
                                                    as subscription_month_sk,
    cast(strftime(state_at_month_end.month_end, '%Y%m%d') as integer)
                                                    as month_end_date_key,
    state_at_month_end.month_start,
    state_at_month_end.subscription_id,
    state_at_month_end.customer_id,
    customers.customer_sk,

    state_at_month_end.plan_name,
    state_at_month_end.billing_period,
    state_at_month_end.subscription_status,
    state_at_month_end.started_on,
    state_at_month_end.cancelled_at,

    -- Only paying subscriptions count toward MRR
    case
        when state_at_month_end.subscription_status in ('active', 'past_due')
            then state_at_month_end.mrr
        else 0
    end                                             as mrr,
    state_at_month_end.subscription_status in ('active', 'past_due')
                                                    as is_paying

from state_at_month_end
left join customers
    -- Customer version at month end. If the customer was deleted during the
    -- month, use their last version before the delete. (least() skips nulls.)
    on  state_at_month_end.customer_id = customers.customer_id
    and customers.valid_from
        < least(state_at_month_end.month_end + interval 1 day, customers.deleted_at)
    and (customers.valid_to
         >= least(state_at_month_end.month_end + interval 1 day, customers.deleted_at)
         or customers.valid_to is null)
where not state_at_month_end.is_deleted
  -- Keep cancelled subscriptions only up to the month they were cancelled
  and not (
      state_at_month_end.subscription_status = 'cancelled'
      and state_at_month_end.cancelled_at < state_at_month_end.month_start
  )
