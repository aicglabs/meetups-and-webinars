---------------------------------------------------
-- AICG 
-- Snowflake Carolinas Meetup
-- 20211025
-- TOPIC: Snowflake Security 
---------------------------------------------------

-- switch to the context of database, wh, role
use role datalakehouse_role;
use database demo_db;
use warehouse datalakehouse_wh;



-- // Create our new schema
create schema IF NOT EXISTS MEETUP_DB_20211025;


-- // Yes, land data to this table via insert / files / Kafka / SnowPipe
create or replace table w_customer_stage (
  id int,
  customer_name string,
  customer_address string,
  customer_postal_code string
);


INSERT INTO w_customer_stage (id, customer_name, customer_address, customer_postal_code)
VALUES
( 1, 'John Conoor', '345 Baker St.', 90210),
( 2, 'Sarah Conoor', '16 Penetentary Drive', 90210)
;

-- verify the table has data
SELECT * FROM w_customer_stage;



-- create roles 
use role securityadmin;
create role if not exists MKTG_RO_ROLE;
create role if not exists MKTG_RW_ROLE;

grant role MKTG_RO_ROLE to role sysadmin;
grant role MKTG_RW_ROLE to role sysadmin;

grant usage on database demo_db to MKTG_RO_ROLE;

grant role MKTG_RO_ROLE to role MKTG_RW_ROLE;

grant role MKTG_RO_ROLE to user aicgmarketing; 

use role MKTG_RO_ROLE;

use role securityadmin;
grant usage on schema MEETUP_DB_20211025 to MKTG_RO_ROLE;

grant select on table MEETUP_DB_20211025.w_customer_stage to MKTG_RO_ROLE;


select * from MEETUP_DB_20211025.w_customer_stage;


grant usage on warehouse datalakehouse_wh to role MKTG_RO_ROLE;


use role securityadmin;
create role MKTG_ADMIN_ROLE;

grant role MKTG_RW_ROLE to role MKTG_ADMIN_ROLE;

use role MKTG_ADMIN_ROLE;
grant role MKTG_ADMIN_ROLE to user aicgmarketing; 




use role sysadmin;

CREATE DATABASE DATASCIENCE_DATABASE


GRANT OWNERSHIP ON DATABASE DS TO ROLE DATASCIENCE_DATABASE_ADMIN_ROLE;


grant usage on warehouse datascience_wh to role DATASCIENCE_DATABASE_ADMIN_ROLE;

//grant usage on warehouse marketing_wh to role DATASCIENCE_DATABASE_ADMIN_ROLE;

update user set default_role="" default_warehouse="" default_namespace="demo_db.meetup_Db_2200000";




























 