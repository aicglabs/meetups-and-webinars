---------------------------------------------------
-- AICG 
-- Snowflake Carolinas Meetup
-- 20210920
-- TOPIC: Snowflake Streams - Continuous Data Pipeline
---------------------------------------------------

-- switch to the context of database, wh, role
use role datalakehouse_role;
use database demo_db;
use warehouse datalakehouse_wh;



-- // Create our new schema
create schema IF NOT EXISTS MEETUP_DB_20210920;


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
 
 
-- // This is the target table for merge
create or replace table w_customer_dimension (
  id int,
  customer_name string,
  customer_address string,
  customer_postal_code string,
  modified_ts datetime default current_timestamp
);
 
 
-- verify the table has data
SELECT * FROM w_customer_dimension;
 

-- // depending on the access you may not have create stage, so perhaps switch to a role with access
use role datalakehouse_role;

-- // Create a stream, and use properties such as append_only if interested only in inserts;
-- 
create or replace stream customer_stage_stream_inserts on table w_customer_stage append_only = true;

create or replace stream customer_stage_stream_updates on table w_customer_stage;

SHOW streams;

-- //reference and discussion point for checking for records in stream if not using append_only
--where metadata$action = 'DELETE';

-- verify the table has data
SELECT * FROM w_customer_dimension;


INSERT INTO w_customer_stage (id, customer_name, customer_address, customer_postal_code)
VALUES
( 3, 'Chales Dyson', '321 Ocean View Dr..', 90210),
( 4, 'Dr. Peter Silberman', '1PP Police Plaza', 90210)
;


 
-- // Periodic merge from staging using CDC. Covers updates & inserts
create task customer_stage_stream_to_rtv_dimension
 warehouse = datalakehouse_wh
 schedule = '2 minutes'
 as
  merge into w_customer_dimension pd
  using 
    customer_stage_stream_inserts delta
    on pd.id = delta.id
  when matched then
  update 
    set pd.customer_name = delta.customer_name, pd.customer_address = delta.customer_address, pd.customer_postal_code = delta.customer_postal_code
  when not matched then
  insert (id, customer_name, customer_address, customer_postal_code)
   values (delta.id, delta.customer_name, delta.customer_address, delta.customer_postal_code)
;

--// let's see the condition of the task and if it was created
SHOW TASKS;


-- resume the task then go to the next step after waiting 2-3 minutes, since tasks are created in suspended
ALTER TASK customer_stage_stream_to_rtv_dimension RESUME;


-- verify the table has data now after the stage data was pulled via the task
SELECT * FROM w_customer_stage;

-- we should see data here
SELECT * FROM w_customer_dimension;



INSERT INTO w_customer_stage (id, customer_name, customer_address, customer_postal_code)
VALUES
( 5, 'Chales Dyson Jr.', '321 Ocean View Dr XX.', 90210),
( 6, 'Dr. Peter Silberman Jr.', '1PP Police Plaza XX', 90210)
;

UPDATE w_customer_stage 
set customer_name = 'Updated Name'
where id = 2
;


-- // get from the stream
select * from customer_stage_stream_inserts;

select * from customer_stage_stream_updates WHERE METADATA$ISUPDATE = TRUE;

INSERT INTO w_customer_stage (id, customer_name, customer_address, customer_postal_code)
VALUES
( 7, 'Chales Dyson III.', '321 Ocean View Dr XX.', 90210),
( 8, 'Dr. Peter Silberman III', '1PP Police Plaza XX', 90210)
;

UPDATE w_customer_stage 
set customer_name = 'Updated Name 3'
where id = 3
;


-- understanding transaction timing... for statements inside of a begin...commit;


--//Cleanup Script
DROP task customer_stage_stream_to_rtv_dimension;
DROP table w_customer_dimension;
DROP table w_customer_stage;

   
   
