---------------------------------------------------
-- AICG 
-- Snowflake Carolinas Meetup
-- 20210823
-- TOPIC: Snowflake Task
---------------------------------------------------

-- change role to securityadmin for user / role steps
use role securityadmin;

-- create the role for administering tasks
create role snow_task_admin;

-- set the active role to ACCOUNTADMIN before granting the EXECUTE TASK privilege to the new role
use role accountadmin;

grant execute task on account to role snow_task_admin;

-- set the active role to SECURITYADMIN to show that this role can grant a role to another role
use role securityadmin;

-- grant the snow_task_admin role to our general DataLakeHouse role
grant role snow_task_admin to role datalakehouse_role;
grant role snow_task_admin to role developer_role;





-- Switch to specifically use this DB in script
USE DATABASE DEMO_DB;
USE WAREHOUSE DATALAKEHOUSE_WH;
//USE ROLE sysadmin;
//DROP SCHEMA MEETUP_DB_20210823;

USE ROLE datalakehouse_role;

-- Create this schema for remainder of script and use it only
CREATE OR REPLACE SCHEMA MEETUP_DB_20210823;

USE SCHEMA MEETUP_DB_20210823;


---------------------------------------------------
-- Create Simple Demo Table
---------------------------------------------------

CREATE OR REPLACE TABLE "DEMO_DB"."MEETUP_DB_20210823".TASK_DEMO (

  ROW_OPTION int,
  ROW_STATUS varchar(50),
  ROW_TS timestamp_tz
 
)
;


---------------------------------------------------
-- Create Simple Demo Stored Procedure
---------------------------------------------------

-- We want to run a basic query that adds a row based on the incremental parameter (ex: 1) from the highest value in the ROW_OPTION column
INSERT INTO DEMO_DB.MEETUP_DB_20210823.TASK_DEMO (ROW_OPTION, ROW_STATUS, ROW_TS)
SELECT NVL(MAX(ROW_OPTION), 0) + 1 , 'OKAY', CURRENT_TIMESTAMP FROM DEMO_DB.MEETUP_DB_20210823.TASK_DEMO
;  

SELECT * FROM DEMO_DB.MEETUP_DB_20210823.TASK_DEMO;


CREATE OR REPLACE PROCEDURE DEMO_DB.MEETUP_DB_20210823.TASK_SPROC_DEMO( p_increment FLOAT ) 
  RETURNS BOOLEAN
  LANGUAGE JAVASCRIPT
  as
  $$
  var sql_command = "INSERT INTO DEMO_DB.MEETUP_DB_20210823.TASK_DEMO (ROW_OPTION, ROW_STATUS, ROW_TS) SELECT NVL(MAX(ROW_OPTION), 0) + 1, 'SPROC_OKAY', CURRENT_TIMESTAMP FROM DEMO_DB.MEETUP_DB_20210823.TASK_DEMO";

  var stmt2 = snowflake.createStatement( { sqlText: sql_command } );
  
  // Execute the SQL statement and store the output (the "result set") in
  // a variable named "rs", which we can access later.
  var rs = stmt2.execute();
  rs.next();
  return rs.getColumnValue(1);
  $$;

CALL TASK_SPROC_DEMO(1);

---------------------------------------------------
-- Create Simple Main Task and other Sub-Tasks
-- This will have the time set for scheduling and be enabled
-- https://docs.snowflake.com/en/sql-reference/sql/create-task.html
---------------------------------------------------

-- Main task
CREATE OR REPLACE TASK TASK_DEMO_MAIN_TASK
  WAREHOUSE = datalakehouse_wh
  SCHEDULE = '2 MINUTE'
  -- SCHEDULE = 'USING CRON 1 * * * * America/New_York' 
  -- TIMESTAMP_INPUT_FORMAT = 'YYYY-MM-DD HH24'
AS
INSERT INTO DEMO_DB.MEETUP_DB_20210823.TASK_DEMO (ROW_OPTION, ROW_STATUS, ROW_TS) 
VALUES (1, 'MAIN_OKAY', CURRENT_TIMESTAMP)
;



-- Sub-task A
CREATE OR REPLACE TASK TASK_DEMO_SUBTASK_A_DEP_MAIN_TASK
  WAREHOUSE = datalakehouse_wh
  AFTER TASK_DEMO_MAIN_TASK
AS
INSERT INTO DEMO_DB.MEETUP_DB_20210823.TASK_DEMO (ROW_OPTION, ROW_STATUS, ROW_TS) 
VALUES (1000, 'SUBTASK_A_OKAY', CURRENT_TIMESTAMP)
;



-- Sub-task B
CREATE OR REPLACE TASK TASK_DEMO_SUBTASK_B_DEP_MAIN_TASK
  WAREHOUSE = datalakehouse_wh
  AFTER TASK_DEMO_MAIN_TASK
AS
INSERT INTO DEMO_DB.MEETUP_DB_20210823.TASK_DEMO (ROW_OPTION, ROW_STATUS, ROW_TS) 
VALUES (2000, 'SUBTASK_B_OKAY', CURRENT_TIMESTAMP)
;



-- Sub-sub-task 
CREATE OR REPLACE TASK TASK_DEMO_SUB_SUBTASK_DEP_SUBTASK_B_TASK
  WAREHOUSE = datalakehouse_wh
  AFTER TASK_DEMO_SUBTASK_B_DEP_MAIN_TASK
AS
CALL TASK_SPROC_DEMO(1)
;


-- let us see the tasks
SHOW TASKS;




---------------------------------------------------
-- Stats, Reviews, and Other Details
---------------------------------------------------
SELECT * FROM DEMO_DB.MEETUP_DB_20210823.TASK_DEMO;

SELECT COUNT(*) FROM DEMO_DB.MEETUP_DB_20210823.TASK_DEMO WHERE ROW_STATUS = 'MAIN_OKAY';

-- how about turning the main sub-sub-task off for a few minutes then back on later
ALTER TASK TASK_DEMO_SUB_SUBTASK_DEP_SUBTASK_B_TASK SUSPEND;


-- now watch the inserts for about 4 minutes
SELECT * FROM DEMO_DB.MEETUP_DB_20210823.TASK_DEMO;


-- resume the task then go to the next step after waiting 2-3 minutes and run select all to catch the increment
ALTER TASK TASK_DEMO_SUB_SUBTASK_DEP_SUBTASK_B_TASK RESUME;
ALTER TASK TASK_DEMO_SUBTASK_A_DEP_MAIN_TASK RESUME;
ALTER TASK TASK_DEMO_MAIN_TASK RESUME;
ALTER TASK TASK_DEMO_SUB_SUBTASK_DEP_SUBTASK_B_TASK RESUME;


-- now resume the last task that calls the sproc, wait 2-3 minutes and run select all to catch the increment
SELECT * FROM DEMO_DB.MEETUP_DB_20210823.TASK_DEMO;

-- lastly a more advanced query to look at the dependency hierarchy of the tasks
select * from table(information_schema.task_dependents(task_name => 'TASK_DEMO_MAIN_TASK', recursive => true));

-- view history for the tasks
-- lots more logic can be inject here for top 10, or timestamp driven queries
-- https://docs.snowflake.com/en/sql-reference/functions/task_history.html#examples
select *
  from table(information_schema.task_history())
  order by scheduled_time;











