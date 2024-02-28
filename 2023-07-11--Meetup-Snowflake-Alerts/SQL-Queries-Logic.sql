--//---------------------------------------------------
--// AICG (AICG.com)
--
--// Meetup Event - Carolina Snowflake Meetup
--// July 11, 2023
--//---------------------------------------------------


--// Set roles, etc.
use role accountadmin;
use role sysadmin;
use warehouse datalakehouse_wh;
use database demo_db;
create schema meetup_db_20230711;
use schema meetup_db_20230711;

--// Create a Notification Integration
--// https://docs.snowflake.com/en/sql-reference/sql/create-notification-integration
--
-- NB: Must have a notification integration in order to send email
-- and the emails to be recipients, ex: in the event_recipients customer table, must be already identified in this
-- notification integration that is called
CREATE OR REPLACE NOTIFICATION INTEGRATION support_team2
    TYPE=EMAIL
    ENABLED=TRUE
    ALLOWED_RECIPIENTS=('username@company.com');


--// create a persisten event logging table
CREATE OR REPLACE TABLE event_log
(
    event_id NUMBER AUTOINCREMENT START 1 INCREMENT 1,
    event_timestamp TIMESTAMP_NTZ,
    event_type STRING,
    event_message STRING,
    event_details VARIANT
);

--// create the event recipients table
CREATE OR REPLACE TABLE event_recipients
(
    event_type STRING,
    notification_integration STRING,
    recipients_email STRING
);

INSERT INTO event_recipients VALUES 
('long_running_query','support_team','notifications@company.com'),
('large_warehouse_resizing','support_team','notifications@company.com'),
('large_warehouse_resizing','support_team','notifications@company.com');


--// create the Snowflake Alert
--// every 1/2 hour check when there is a long running query that runs for more than 15 minutes (900000)
CREATE OR REPLACE ALERT alert_long_running_unsuccessful_queries
  WAREHOUSE = alerting_wh
  SCHEDULE = '30 MINUTE'
  IF (EXISTS (
         SELECT 1
         FROM         
         TABLE(INFORMATION_SCHEMA.QUERY_HISTORY
         (DATEADD('HOUR',-1,CURRENT_TIMESTAMP()),CURRENT_TIMESTAMP()))
         WHERE execution_status!='SUCCESS'
         AND total_elapsed_time>900000
         AND start_time BETWEEN 
            IFNULL(SNOWFLAKE.ALERT.LAST_SUCCESSFUL_SCHEDULED_TIME(),
            '1900-01-01'::TIMESTAMP_NTZ) AND SNOWFLAKE.ALERT.SCHEDULED_TIME()
    )
  )
  THEN 
  INSERT INTO event_log
    (event_timestamp,event_type,event_message,event_details)  
  SELECT 
    SNOWFLAKE.ALERT.SCHEDULED_TIME(),
    'long_running_query',
    'Long running query ('||query_id||') detected.',
    OBJECT_CONSTRUCT(
    'query_id',query_id,
    'query_type',query_type,
    'start_time',start_time,
    'user_name',user_name,
    'warehouse_name',warehouse_name,
    'end_time',end_time,
    'total_elapsed_time',total_elapsed_time,
    'execution_status',execution_status,
    'error_code',error_code,
    'error_message',error_message
    )
FROM 
TABLE(INFORMATION_SCHEMA.QUERY_HISTORY
(DATEADD('HOUR',-1,CURRENT_TIMESTAMP()),CURRENT_TIMESTAMP()))
WHERE execution_status!='SUCCESS'
AND total_elapsed_time>900000
AND start_time BETWEEN 
  IFNULL(SNOWFLAKE.ALERT.LAST_SUCCESSFUL_SCHEDULED_TIME(),
  '1900-01-01'::TIMESTAMP_NTZ) AND SNOWFLAKE.ALERT.SCHEDULED_TIME()
ORDER BY start_time;

--// Create another Alert
--// every 4 hours check when someone increases the warehouse size TO an XLARGE
CREATE OR REPLACE ALERT alert_warehouse_resize_event
  WAREHOUSE = alerting_wh
  SCHEDULE = '240 MINUTE'
  IF (EXISTS (
     SELECT 
         1
     FROM snowflake.account_usage.warehouse_events_history eh 
     INNER JOIN snowflake.account_usage.query_history qh 
     ON eh.query_id = qh.query_id 
     WHERE 
      event_reason='WAREHOUSE_RESIZE' 
      AND event_name='RESIZE_WAREHOUSE' 
      AND event_state='COMPLETED'
     AND timestamp BETWEEN 
        IFNULL(SNOWFLAKE.ALERT.LAST_SUCCESSFUL_SCHEDULED_TIME(),
        '1900-01-01'::TIMESTAMP_NTZ)
     AND SNOWFLAKE.ALERT.SCHEDULED_TIME()
     AND CONTAINS(LOWER(query_text),'xlarge')  
      )
  )
  THEN 
  INSERT INTO event_log
    (event_timestamp,event_type,event_message,event_details)   
  WITH cte AS 
  (
    SELECT 
      eh.timestamp,
      eh.warehouse_name,
      eh.event_name,
      eh.event_reason,
      eh.query_id,
      qh.query_text,
      qh.user_name
    FROM snowflake.account_usage.warehouse_events_history eh 
    INNER JOIN snowflake.account_usage.query_history qh 
    ON eh.query_id = qh.query_id 
    WHERE event_reason='WAREHOUSE_RESIZE' 
    AND event_name='RESIZE_WAREHOUSE' 
    AND event_state='COMPLETED'
    AND timestamp BETWEEN 
      IFNULL(SNOWFLAKE.ALERT.LAST_SUCCESSFUL_SCHEDULED_TIME(),
      '1900-01-01'::TIMESTAMP_NTZ)
    AND SNOWFLAKE.ALERT.SCHEDULED_TIME()
    AND CONTAINS(LOWER(query_text),'xlarge')
)
SELECT 
    SNOWFLAKE.ALERT.SCHEDULED_TIME(),
    'large_warehouse_resizing',
    'Warehouse '||warehouse_name||
    ' has been resized by '||user_name||
    ' to XLARGE or higher.',
    OBJECT_CONSTRUCT(
       'query_id',query_id,
       'start_time',timestamp,
       'user_name',user_name,
       'warehouse_name',warehouse_name
    )
FROM
cte;




--// Create stored procedure to send the email using Snowpark
CREATE OR REPLACE PROCEDURE proc_send_email(
  event_recipients_table VARCHAR,
  event_log_table VARCHAR,
  timestamp TIMESTAMP_NTZ)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
AS
$$

import snowflake.snowpark as snowpark

#-- //to test on local dev jupyter notebook

#-- //1. establish credentials connection to our snowflake account
#-- SnowflakeDriver snowConn = new connection(username, password);

#-- //2. create snowpark connection
#-- SnowParkSession snowfparkSession = new snowpark(snowConn);

#-- //3. at the bottom call the main function using 
#-- main(snowfparkSession, 'tablea', 'tableb');


def main(session: snowpark.Session,event_recipients_table,event_log_table,timestamp): 
    df_email_messages = session.sql("""
        WITH cte_event_recipients
          (event_recipients_table,
           event_type,
           notification_integration,
           recipients_email) 
        AS
        (
            SELECT
                event_type,
                notification_integration,
                LISTAGG(recipients_email,',')
            FROM {0}
            GROUP BY event_type,notification_integration
        )        
        ,cte_events(event_type,event_message) AS 
        (
            SELECT                        
                event_type,
                'Event ID:'||event_id::STRING||' -> '||event_message                    
            FROM {1}
            WHERE event_timestamp>'{2}'::DATE
        )
        SELECT 
            er.event_type email_subject,
            er.notification_integration,
            er.recipients_email,
            LISTAGG(e.event_message||'\n') email_body
        FROM cte_events e
        INNER JOIN cte_event_recipients er 
        ON e.event_type=er.event_type
        GROUP BY 1,2,3
    """.format(event_recipients_table,event_log_table,timestamp)).to_pandas()

    for idx,row in df_email_messages.iterrows():
        session.sql("""
        CALL SYSTEM$SEND_EMAIL(
        '{notification_integration}',
        '{recipients_email}',
        '{email_subject}',
        '{email_body}'
        )
        """.format(notification_integration = row["NOTIFICATION_INTEGRATION"],
                   recipients_email = row["RECIPIENTS_EMAIL"],
                   email_subject = row["EMAIL_SUBJECT"],
                   email_body = row["EMAIL_BODY"]
                  )).collect()
    
    return "Sent!"
$$;


--// create alert for email events
--// Alerts are similar to Snowflake TASKS - with the exception of IF...THEN
--// Questions:  is there an IF...THEN...ELSE?  It does not appear to currently support ELSE, possibly use IF NOT EXISTS(...)
CREATE OR REPLACE ALERT alert_email_events
  WAREHOUSE = alerting_wh
  SCHEDULE = '60 MINUTE'
  IF (EXISTS ( 
        SELECT *
        FROM event_log        
        WHERE event_timestamp BETWEEN IFNULL(SNOWFLAKE.ALERT.LAST_SUCCESSFUL_SCHEDULED_TIME(),'1970-01-01'::TIMESTAMP_NTZ)
        AND SNOWFLAKE.ALERT.SCHEDULED_TIME()
      )
  )
  THEN CALL PROC_SEND_EMAIL(
    'EVENT_RECIPIENTS',
    'EVENT_LOG',
    SNOWFLAKE.ALERT.SCHEDULED_TIME()::TIMESTAMP_NTZ)
;


--// set alert as resumed and runnable
--// if not in RESUME then the status
ALTER ALERT alert_email_events RESUME;

--// show all alerts Created
SHOW ALERTS;



--// Clean up Steps
drop schema meetup_db_2023;

