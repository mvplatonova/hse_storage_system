--
-- Hubs
--

-- HUB_CUSTOMER
create table hub_customer (
    customer_key serial primary key,
    customer_id text not null,
    load_date timestamp default now(),
    record_source text
) distributed by (customer_key);

-- HUB_PRODUCT
create table hub_product (
    product_key serial primary key,
    sub_category text not null,
    load_date timestamp default now(),
    record_source text
) distributed by (product_key);

-- HUB_ORDER
create table hub_order (
    order_key serial primary key,
    order_id text not null,
    load_date timestamp default now(),
    record_source text
) distributed by (order_key);

--
-- Satellites
--

-- SAT_CUSTOMER_DETAILS
create table sat_customer_details (
    sat_customer_key serial primary key,
    customer_key int references hub_customer(customer_key),
    segment text,
    country text,
    city text,
    state text,
    region text,
    postal_code text,
    load_date timestamp default now(),
    record_source text
) distributed by (sat_customer_key);

-- SAT_PRODUCT_DETAILS
create table sat_product_details (
    sat_product_key serial primary key,
    product_key int references hub_product(product_key),
    category text,
    load_date timestamp default now(),
    record_source text
) distributed by (sat_product_key);

-- SAT_ORDER_DETAILS
create table sat_order_details (
    sat_order_key serial primary key,
    order_key int references hub_order(order_key),
    ship_mode text,
    load_date timestamp default now(),
    record_source text
) distributed by (sat_order_key);

-- SAT_SALES_DETAILS
create table sat_sales_details (
    sat_sales_key serial primary key,
    link_sales_key int references link_sales(link_sales_key),
    sales numeric(10,2),
    quantity int,
    discount numeric(5,2),
    profit numeric(10,2),
    load_date timestamp default now(),
    record_source text
) distributed by (sat_sales_key);

--
-- Links
--

-- LINK_SALES
create table link_sales (
    link_sales_key serial primary key,
    order_key int references hub_order(order_key),
    customer_key int references hub_customer(customer_key),
    product_key int references hub_product(product_key),
    load_date timestamp default now(),
    record_source text
) distributed by (link_sales_key);
