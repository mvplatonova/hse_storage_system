-- EXTRAS
create table tmp_data (
    order_id text,
    customer_id text,
    segment text,
    country text,
    city text,
    state text,
    postal_code text,
    region text,
    category text,
    sub_category text,
    ship_mode text,
    sales numeric(10,2),
    quantity int,
    discount numeric(5,2),
    profit numeric(10,2)
);

\copy tmp_data from 'dataset.csv' CSV HEADER;

--
-- Hubs
--

-- HUB_CUSTOMER
insert into hub_customer (customer_id, record_source)
select distinct
    customer_id,
    'data.csv'
from tmp_data
where customer_id is not null;

-- HUB_PRODUCT
insert into hub_product (sub_category, record_source)
select distinct
    sub_category,
    'data.csv'
from tmp_data
where sub_category is not null;

-- HUB_ORDER
insert into hub_order (order_id, record_source)
select distinct
    order_id,
    'data.csv'
from tmp_data
where order_id is not null;

--
-- Links
--

-- LINK_SALES
insert into link_sales (order_key, customer_key, product_key, record_source)
select distinct
    ho.order_key,
    hc.customer_key,
    hp.product_key,
    'data.csv'
from tmp_data t
join hub_order ho on ho.order_id = t.order_id
join hub_customer hc on hc.customer_id = t.customer_id
join hub_product hp on hp.sub_category = t.sub_category;


--
-- Satellites
--

-- SAT_CUSTOMER_DETAILS
insert into sat_customer_details (
    customer_key, segment, country, city, state, region, postal_code, record_source
)
select distinct
    hc.customer_key,
    t.segment,
    t.country,
    t.city,
    t.state,
    t.region,
    t.postal_code,
    'data.csv'
from tmp_data t
join hub_customer hc on hc.customer_id = t.customer_id;

-- SAT_PRODUCT_DETAILS
insert into sat_product_details (
    product_key, category, record_source
)
select distinct
    hp.product_key,
    t.category,
    'data.csv'
from tmp_data t
join hub_product hp on hp.sub_category = t.sub_category;

-- SAT_ORDER_DETAILS
insert into sat_order_details (
    order_key, ship_mode, record_source
)
select distinct
    ho.order_key,
    t.ship_mode,
    'data.csv'
from tmp_data t
join hub_order ho on ho.order_id = t.order_id;

-- SAT_SALES_DETAILS
insert into sat_sales_details (
    link_sales_key, sales, quantity, discount, profit, record_source
)
select
    ls.link_sales_key,
    t.sales,
    t.quantity,
    t.discount,
    t.profit,
    'data.csv'
from tmp_data t
join hub_order ho on ho.order_id = t.order_id
join hub_customer hc on hc.customer_id = t.customer_id
join hub_product hp on hp.sub_category = t.sub_category
join link_sales ls 
    on ls.order_key = ho.order_key 
   and ls.customer_key = hc.customer_key 
   and ls.product_key = hp.product_key;


-- EXTRAS
drop table tmp_data;


