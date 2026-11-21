---
title: E-commerce metrics
---

Every number here comes from the dbt semantic layer (`models/semantic/`):
`scripts/export_metrics.py` runs `mf query` for each dataset, so the definitions
of MRR, churn and revenue live in one place. The data is synthetic.

```sql subscriptions
select
    cast(metric_time__month as date) as month,
    mrr,
    net_new_mrr,
    paying_subscriptions,
    churn_rate
from metrics.monthly_subscriptions
order by month
```

```sql latest
select * from ${subscriptions} order by month desc limit 1
```

```sql revenue
select
    cast(metric_time__month as date) as month,
    recognized_revenue,
    orders,
    average_order_value
from metrics.monthly_revenue
order by month
```

```sql trials
select
    cast(metric_time__quarter as date) as quarter,
    trial_conversions,
    trial_endings,
    trial_conversion_rate
from metrics.quarterly_trials
order by quarter
```

<BigValue data={latest} value=mrr title="MRR (latest month)" fmt=usd0 />
<BigValue data={latest} value=paying_subscriptions title="Paying subscriptions" />
<BigValue data={latest} value=churn_rate title="Churn rate (latest month)" fmt=pct1 />

## Subscriptions

<LineChart data={subscriptions} x=month y=mrr title="MRR by month" yFmt=usd0 />

<BarChart data={subscriptions} x=month y=net_new_mrr title="Net new MRR by month" yFmt=usd0 />

<LineChart
    data={subscriptions}
    x=month
    y=churn_rate
    title="Churn rate (churned in the month / paying at its start)"
    yFmt=pct1
/>

## Orders

<BarChart data={revenue} x=month y=recognized_revenue title="Recognized revenue by month" yFmt=usd0 />

<LineChart data={revenue} x=month y=average_order_value title="Average order value" yFmt=usd2 />

## Trials

<BarChart data={trials} x=quarter y=trial_conversion_rate title="Trial conversion rate by quarter" yFmt=pct0 />
