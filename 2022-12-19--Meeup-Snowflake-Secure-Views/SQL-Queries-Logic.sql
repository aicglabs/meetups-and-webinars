---------------------------------------------------
-- AICG 
-- Snowflake Carolinas Meetup
-- 20221219
-- TOPIC: Secure Views in Snowflake
---------------------------------------------------

use role demo_role;
use warehouse datalakehouse_wh;
use database demo_db;

create schema demo_db.meetup_20221219;

show views;

create table w_sales_fact (
    product_name varchar,
    sales_region varchar(50),
    sales_team varchar(50),
    price float default 0.00,
    qty number(38,0) default 0,
    created_on timestamp_ltz(9) default current_timestamp
)
;

INSERT INTO w_sales_fact (product_name, sales_region, sales_Team, price, qty)
SELECT 'New Gear', 'NEVADA', 'WEST', 99, 2
UNION ALL SELECT 'Deal Flow Product', 'NEVADA', 'WEST', 199, 1
UNION ALL SELECT 'Flowbee', 'COLORADO', 'WEST', 399, 100
UNION ALL SELECT 'Matrix Tickets', 'NEVADA', 'WEST', 19, 1000
UNION ALL SELECT 'Sand Castles', 'VIRGINIA', 'MID-ATLANTIC', 1999, 1
UNION ALL SELECT 'Castle Corona Visit', 'VIRGINIA', 'MID-ATLANTIC', 333, 1
UNION ALL SELECT 'Amazing Swing Set Home', 'NORTH CAROLINA', 'MID-ATLANTIC', 1999, 1
UNION ALL SELECT 'Disney Cruise', 'FLORIDA', 'MID-ATLANTIC', 9999, 10
UNION ALL SELECT 'Disney Open House', 'FLORIDA', 'MID-ATLANTIC', 7799, 6
UNION ALL SELECT 'Vegas Excursion', 'NEW MEXICO', 'WEST', 190099, 3
UNION ALL SELECT 'California Dreaming Yoga Retreat', 'CALIFORNIA', 'WEST', 599, 3
;

SELECT * FROM w_sales_fact;


-- create a basic access control table
create table r_sales_team_rbac (
    lowest_grain_access_ref varchar,
    access_role varchar
)
;

INSERT INTO r_sales_team_rbac (lowest_grain_access_ref, access_role)
SELECT 'VIRGINIA', 'DEMO_ROLE'
UNION ALL SELECT 'CALIFORNIA', 'PUBLIC'
UNION ALL SELECT 'COLORADO', 'PUBLIC'
UNION ALL SELECT 'NEW MEXICO', 'PUBLIC'
UNION ALL SELECT 'NEVADA', 'PUBLIC'
UNION ALL SELECT 'FLORIDA', 'BOSS_LEVEL_ROLE'
;

select * from r_sales_team_rbac;


grant usage on schema demo_db.meetup_20221219 to role public;
// grant select on all tables in schema demo_db.meetup_20221219 to role public;
//grant select on future tables in schema  demo_db.meetup_20221219 to role public;
grant select on all views in schema  demo_db.meetup_20221219 to role public;
//grant select on future views in schema demo_db.meetup_20221219 to role public;



create or replace secure view vw_sales_fact as

select * from w_sales_fact a 

where a.sales_region in (
  
    select lowest_grain_access_ref
    from r_sales_team_rbac b
    where upper(access_role) = current_role()
  
)
;




SELECT * FROM vw_sales_fact;


use role public;

select current_role();


SELECT * FROM vw_sales_fact;


use role demo_role;

grant select on all views in schema  demo_db.meetup_20221219 to role public;

use role public;

select current_role();

SELECT * FROM vw_sales_fact;









drop view vw_sales_fact;
drop table r_sales_team_rbac;
drop table w_sales_fact;

































