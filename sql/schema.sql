-- Dimensional (star) schema for the retail ETL pipeline.
-- Fact grain: one row per invoice line.
-- Dimensions: customer, product, date.
--
-- NOTE: this supersedes the flat customer_aggregates TABLE from the load stage.
-- Run it together with the updated load Lambda (which populates the star
-- schema); customer_aggregates is recreated below as a VIEW on top of the fact.

-- --- DIMENSIONS ---
create table if not exists dim_customer (
    customer_key bigint generated always as identity primary key,
    customer_id  bigint not null unique,
    country      text
);

create table if not exists dim_product (
    product_key bigint generated always as identity primary key,
    stock_code  text not null unique,
    description text
);

create table if not exists dim_date (
    date_key    integer primary key,   -- YYYYMMDD
    full_date   date    not null,
    year        integer not null,
    month       integer not null,
    day         integer not null,
    day_of_week integer not null       -- 0 = Monday .. 6 = Sunday
);

-- --- FACT ---
create table if not exists fact_sales (
    sales_id     bigint generated always as identity primary key,
    invoice_no   text    not null,
    customer_key bigint  not null references dim_customer(customer_key),
    product_key  bigint  not null references dim_product(product_key),
    date_key     integer not null references dim_date(date_key),
    quantity     integer not null,
    unit_price   numeric(12,2) not null,
    line_total   numeric(14,2) not null
);

create index if not exists ix_fact_customer on fact_sales(customer_key);
create index if not exists ix_fact_product  on fact_sales(product_key);
create index if not exists ix_fact_date     on fact_sales(date_key);

-- --- DERIVED MART ---
-- Customer summary, now expressed as a view over the star schema.
drop table if exists customer_aggregates cascade;

create or replace view customer_aggregates as
select
    c.customer_id,
    c.country,
    count(distinct f.invoice_no)  as n_orders,
    sum(f.quantity)               as n_items,
    round(sum(f.line_total), 2)   as total_spend,
    min(d.full_date)              as first_purchase,
    max(d.full_date)              as last_purchase
from fact_sales f
join dim_customer c on c.customer_key = f.customer_key
join dim_date     d on d.date_key     = f.date_key
group by c.customer_id, c.country;
