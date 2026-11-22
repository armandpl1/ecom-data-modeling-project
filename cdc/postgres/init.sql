-- Source system for the CDC path: the operational tables the generator's
-- change history is replayed into (scripts/cdc_source.py), captured by
-- Debezium. REPLICA IDENTITY FULL makes delete events carry the whole row.

create table customers (
    customer_id      text primary key,
    first_name       text,
    last_name        text,
    email            text,
    phone            text,
    street_address   text,
    city             text,
    state            text,
    postal_code      text,
    country          text,
    customer_segment text,
    marketing_opt_in boolean,
    created_at       timestamp,
    updated_at       timestamp
);

create table products (
    product_id   text primary key,
    sku          text,
    product_name text,
    category     text,
    unit_price   numeric(12, 2),
    currency     text,
    is_active    boolean,
    created_at   timestamp,
    updated_at   timestamp
);

create table orders (
    order_id        text primary key,
    customer_id     text,
    product_id      text,
    quantity        integer,
    unit_price      numeric(12, 2),
    discount_pct    numeric(12, 2),
    order_total     numeric(12, 2),
    currency        text,
    payment_method  text,
    status          text,
    order_placed_at timestamp,
    updated_at      timestamp
);

create table subscriptions (
    subscription_id text primary key,
    customer_id     text,
    plan_name       text,
    billing_period  text,
    mrr             numeric(12, 2),
    status          text,
    started_at      date,
    trial_ends_at   date,
    cancelled_at    timestamp,
    created_at      timestamp,
    updated_at      timestamp
);

alter table customers     replica identity full;
alter table products      replica identity full;
alter table orders        replica identity full;
alter table subscriptions replica identity full;

-- How far the replay has got (not captured: Debezium only includes the four
-- tables above).
create table _replay_state (
    id      integer primary key default 1 check (id = 1),
    applied integer not null
);
insert into _replay_state values (1, 0);
