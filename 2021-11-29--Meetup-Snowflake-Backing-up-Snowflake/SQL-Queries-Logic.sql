---------------------------------------------------
-- AICG 
-- Snowflake Carolinas Meetup
-- 20211129
-- TOPIC: Backing Up Snowflake
---------------------------------------------------

-- switch to the context of database, wh, role
use role datalakehouse_role;
use database demo_db;
use warehouse datalakehouse_wh;



-- // Create a backup Database
use role sysadmin;
CREATE DATABASE BACKUP_DATABASE COMMENT="Database used for storing scheduled backups of schemas or tables, usually based on a Snowflake Tasks";



use role securityadmin;
use database BACKUP_DATABASE;
GRANT ALL PRIVILEGES ON SCHEMA BACKUP_DATABASE.PUBLIC TO ROLE DATALAKEHOUSE_ROLE;


-- // Create a procedure 
use role sysadmin;
-- // PREPARE SCRIPT FOR AN ONGOING TASK FOR BACKUP LOGIC
CREATE OR REPLACE PROCEDURE BACKUP_DATABASE.PUBLIC.SP_MONTHLY_BACKUP_TASK_BY_FQDN( p_fqdn_schema_name STRING ) 
  RETURNS STRING
  LANGUAGE JAVASCRIPT
  -- EXECUTE AS CALLER
  AS $$
  
  let today = new Date();

  var vTodayStr = today.toISOString().split('T')[0];

  var sql_command = "";
  

  var pieces = P_FQDN_SCHEMA_NAME.split('.');
  if(pieces.length == 2)
  {
  
    sql_command = "CREATE SCHEMA BACKUP_DATABASE." + pieces[1] + '_' + vTodayStr.split('-').join('') + " CLONE " + P_FQDN_SCHEMA_NAME + ";";
    
    sql_command_grant = "GRANT SELECT ON ALL TABLES IN SCHEMA BACKUP_DATABASE." + pieces[1] + '_' + vTodayStr.split('-').join('') + " TO ROLE DATALAKEHOUSE_ROLE;";

    //console.log(pieces[1]);

    var stmt2 = snowflake.createStatement( { sqlText: sql_command } );
    var stmt3 = snowflake.createStatement( { sqlText: sql_command_grant } );
    

    // Execute the SQL statement and store the output (the "result set") in
    // a variable named "rs", which we can access later.
    var rs = stmt2.execute();
    
    // execute the grant
    stmt3.execute();
  }

  return sql_command;
  $$;
  
  
  
--// conduct a test for backup
CALL BACKUP_DATABASE.PUBLIC.SP_MONTHLY_BACKUP_TASK_BY_FQDN('DLH_DEMO_SANDBOX.ADVWORKS_DBO');
 
 
-- drop if needed
use role sysadmin;
DROP SCHEMA "DEMO_DB"."ADVWORKS_DBO";




-- assign permissions for this role to manage tasks

-- change role to securityadmin for user / role steps
use role securityadmin;

-- create the role for administering tasks
create role snowflake_task_admin;

-- set the active role to ACCOUNTADMIN before granting the EXECUTE TASK privilege to the new role
use role accountadmin;

grant execute task on account to role snowflake_task_admin;

-- set the active role to SECURITYADMIN to show that this role can grant a role to another role
use role securityadmin;

-- grant the snow_task_admin role to our general DataLakeHouse role
grant role snowflake_task_admin to role DATALAKEHOUSE_ROLE;
grant role snowflake_task_admin to role SYSADMIN;

grant usage on warehouse datalakehouse_wh to role snowflake_task_admin;

use database BACKUP_DATABASE;

grant usage on all procedures in schema public to role snowflake_task_admin;
grant usage on procedure BACKUP_DATABASE.PUBLIC.SP_MONTHLY_BACKUP_TASK_BY_FQDN(string) to role snowflake_task_admin;


-- 
use role DATALAKEHOUSE_ROLE;
//DROP TASK PUBLIC.MONTHLY_BACKUP_TASK;
-- Main task for backup 
CREATE OR REPLACE TASK PUBLIC.MONTHLY_BACKUP_TASK__ADVWORKS_DBO
  WAREHOUSE = datalakehouse_wh
  SCHEDULE = 'USING CRON 0 0 1 * * America/New_York' 
  -- SCHEDULE = 'USING CRON 0 0 12 * * America/New_York' 
  -- TIMESTAMP_INPUT_FORMAT = 'YYYY-MM-DD HH24'
AS
    // ref: https://crontab.guru/#0_0_12_*_*
    // ref: https://docs.snowflake.com/en/sql-reference/functions/task_history.html
    // NB: Can only call one SQL line of execution or stored procedure call in a single task
    CALL BACKUP_DATABASE.PUBLIC.SP_MONTHLY_BACKUP_TASK_BY_FQDN('DLH_DEMO_SANDBOX.ADVWORKS_DBO');
;

-- show the task and current status
SHOW TASK BACKUP_DATABASE.PUBLIC.MONTHLY_BACKUP_TASK__ADVWORKS_DBO
-- enable/turn-on the tasks to run by using resume
ALTER TASK BACKUP_DATABASE.PUBLIC.MONTHLY_BACKUP_TASK__ADVWORKS_DBO RESUME;





---------------------------------------------------
--
-- Other Scenarios for Backing Up Snowflake
-- Using External Stages between Snowflake Accounts
-- 
---------------------------------------------------

--// export to external stage
COPY INTO @"DADATABASE"."DASCHEMA"."DASTAGE"/backup/ERRORLOG.csv 
FROM ( SELECT * FROM ERRORLOG) FILE_FORMAT = (TYPE = 'CSV', FIELD_OPTIONALLY_ENCLOSED_BY = '"') HEADER = true;


-- on target account, create DDL prior to loading from external stage
 
--// Pull from an External Stage into the local Snowflake Account
COPY INTO DEVELOPER_SANDBOX.ERRORLOG 
FROM @DLH_S3_STAGE_MODEL_DATA/backup/ERRORLOG.csv FILE_FORMAT = ( TYPE = CSV, SKIP_HEADER = 1, COMPRESSION = GZIP, FIELD_OPTIONALLY_ENCLOSED_BY ='"' );

 
 
 
 

