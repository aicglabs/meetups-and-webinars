-// -------------------------------------

--// AICG.com

--// Snowflake Meetup

--// January 30, 2023

--// -------------------------------------


use role accountadmin;
    
    
use database demo_db;
use schema ICEBERG_DEMO;



-- 1. Create External Volume 
create or replace external volume iceberg_ext_vol_3
STORAGE_LOCATIONS = 
(
	(
		NAME = 'my-gcs-us-central1'
		STORAGE_PROVIDER = 'GCS'
		STORAGE_BASE_URL = 'gcs://iceberg_sample_bucket/parque_test/'
	)
);



-- 2. Describe External Volume 
DESC EXTERNAL VOLUME iceberg_ext_vol_3;



-- 3. Create a storage integration for an external stage
create or replace storage integration iceberg_gcs_int
  type = external_stage
  storage_provider = 'GCS'
  enabled = true
  storage_allowed_locations = ('gcs://iceberg_sample_bucket');
    
    
    
--SHOW INTEGRATIONS;


    
-- 4. create an external stage to prepare to load our parquet file (instead of direct iceberg file)
create or replace stage iceberg_ext_stage
  url='gcs://iceberg_sample_bucket' --// URL required for external stage (vs normal/internal stage)
  storage_integration = iceberg_gcs_int
  file_format = (TYPE = PARQUET);
  
  
  
-- 5. create an EXTERNAL table based on the EXTERNAL stage
create or replace external table parquet_test (
    first_name varchar as (value:first_name::varchar)
)
--integration = iceberg_gcs_int -- NB: only needed (a notification integration concept) if wanting auto_refresh as true
location=@iceberg_ext_stage/parque_test/
auto_refresh = false  --NB: must be false to avoid requiring notification integration
file_format = (type = parquet);
  
  
  
-- XX. how you could create a ICEBERG table based on external volume location of an ICEBERG file
//drop table if exists parquet_test_ice;
//create or replace iceberg table parquet_test_ice (
//    first_name varchar
//)
//with EXTERNAL_VOLUME = iceberg_ext_vol_3
//;
  


-- 6. Test the external table 
SELECT * from parquet_test limit 1000;



-- 7. Create data in our bucket from the EXTERNAL table
create or replace iceberg table my_iceberg_table_3
  with EXTERNAL_VOLUME = 'iceberg_ext_vol_3'
  as select * from parquet_test;



-- 8. Query the ICEBERG table
SELECT * FROM my_iceberg_table_3 LIMIT 1000;



-- clean up
DROP TABLE DEMO_DB.ICEBERG_DEMO.MY_ICEBERG_TABLE_3;

 