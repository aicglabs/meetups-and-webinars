#
# AICG
# DataLakeHouse.io
#
# Snowflake Meetup 20210621
#   - Using External & Internal Stages
#
#

-- Just switch to the main database in discussion
USE DATABASE SNOWFLAKE_MEETUP_SANDBOX;
USE WAREHOUSE SNOWFLAKE_MEETUP_WAREHOUSE;


-- Use security admin here to backfill any missing permissions that would not be given to a new user on a DB, etc.
USE ROLE SECURITYADMIN;
GRANT USAGE ON SCHEMA "SNOWFLAKE_MEETUP_SANDBOX"."PUBLIC" TO ROLE DEVELOPER_ROLE;
GRANT CREATE TABLE ON SCHEMA "SNOWFLAKE_MEETUP_SANDBOX"."PUBLIC" TO ROLE DEVELOPER_ROLE;
GRANT SELECT ON ALL TABLES IN SCHEMA "SNOWFLAKE_MEETUP_SANDBOX"."PUBLIC" TO ROLE DEVELOPER_ROLE;
grant select on future tables in schema "SNOWFLAKE_MEETUP_SANDBOX"."PUBLIC" TO ROLE FIVETRAN_ROLE;
GRANT INSERT, UPDATE, DELETE, TRUNCATE ON ALL TABLES IN SCHEMA "SNOWFLAKE_MEETUP_SANDBOX"."PUBLIC" TO ROLE DEVELOPER_ROLE;
GRANT INSERT, UPDATE, DELETE, TRUNCATE ON FUTURE TABLES IN SCHEMA "SNOWFLAKE_MEETUP_SANDBOX"."PUBLIC" TO ROLE DEVELOPER_ROLE;



# SOME BASIC setup for table structure for this meetup and testings
USE ROLE DEVELOPER_ROLE;

-- Switch Schema
USE SCHEMA PUBLIC;

-- Create a table that will be used for both internal and external stage testing
CREATE OR REPLACE TABLE YEAR_DAYS (

  "DATE" DATE
  , DAYOFWEEK VARCHAR(9)
  
)
;


SELECT * FROM YEAR_DAYS;



# INTERNAL ('Named' Internal) STAGE -------------------------------

-- using the UI to show how straightforward it is
-- 1. Go Databases option in ribbon
-- 2. Select database where stage should be
-- 3. Click the Stages tab
-- 4. Create a stage using the Snowflake Managed option
-- 5. return here







# EXTERNAL STAGE -------------------------------

-- Role of account admin
USE ROLE ACCOUNTADMIN;



-- Create the storate integration (https://docs.snowflake.com/en/sql-reference/sql/create-storage-integration.html)
CREATE OR REPLACE STORAGE INTEGRATION MEETUP_STORAGE_STG_INTG_GCP 
TYPE = EXTERNAL_STAGE
STORAGE_PROVIDER = GCS 
ENABLED = TRUE 
STORAGE_ALLOWED_LOCATIONS = ('gcs://snowflake-meetup-test/');



-- Describe the integration for storage that was just created
DESC INTEGRATION MEETUP_STORAGE_STG_INTG_GCP;



-- Create a stage as the integration point to access the storage integration's details (ex: connector from snowflakes svc account on the vendor)
CREATE STAGE "SNOWFLAKE_MEETUP_SANDBOX"."PUBLIC".MEETUP_STORAGE_STAGE_TO_GCP
STORAGE_INTEGRATION = MEETUP_STORAGE_STG_INTG_GCP
URL = 'gcs://snowflake-meetup-tes2' 
-- file_format = (type = 'CSV' field_delimiter = '|' skip_header = 1);
COMMENT = 'Stage that uses storage integration to interact with the GCP storage bucket define';



-- List/Show the stage by object name with the '@' (it hould fail until you grant the snowflake svc account access on the bucket)
LIST @MEETUP_STORAGE_STAGE_TO_GCP;



-- switch to developer role just as a checkpoint validation that any normal user (non sysadmin or non accountadmin) can run this
USE ROLE DEVELOPER_ROLE;


-- truncate our target table just to prove it is currently empty
TRUNCATE TABLE YEAR_DAYS;


-- Use COPY INTO to load from the external stage (DID YOU GET A FAILURE????? If so go to next line to run GRANT to prevent)
--  NB: 
--      Notice there is no FORMAT, no PATTERN used
COPY INTO YEAR_DAYS 
FROM @MEETUP_STORAGE_STAGE_TO_GCP;



/*
COPY INTO YEAR_DAYS 
FILE_FORMAT = (format_name = csvtotableformat)
PATTERN ='.*years.*[.]csv'
FROM @MEETUP_STORAGE_STAGE_TO_GCP;
*/


-- must change to ACCOUNTADMin to GRANT a STAGE to any role
USE ROLE ACCOUNTADMIN;
GRANT USAGE ON STAGE MEETUP_STORAGE_STAGE_TO_GCP TO ROLE DEVELOPER_ROLE;


-- test again for accuracy
LIST @MEETUP_STORAGE_STAGE_TO_GCP;

COPY INTO YEAR_DAYS 
FROM @MEETUP_STORAGE_STAGE_TO_GCP;



-- validate data was loaded from external stage
SELECT * FROM YEAR_DAYS;


-- create a new table from the previous one just loaded
CREATE TABLE YEAR_DAYS_EXTRA AS SELECT *, 'A' AS NEW_COL1, 101 AS NEW_COL2 
FROM YEAR_DAYS;


-- view it
SELECT * FROM YEAR_DAYS_EXTRA;



-- move data into the external stage from this new table
COPY INTO @MEETUP_STORAGE_STAGE_TO_GCP/new_folder/ from YEAR_DAYS_EXTRA;
------ if you received, Failed to access remote file: access denied. Please check your credentials, you have privileges set as read-only



############### REVERSE ALL OBJECT CREATION ##################


















