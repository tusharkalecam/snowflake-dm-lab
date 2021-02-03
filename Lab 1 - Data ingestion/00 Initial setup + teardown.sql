USE ROLE DBA;

CREATE DATABASE SNOWFLAKE_DM_LAB;

GRANT USAGE ON DATABASE SNOWFLAKE_DM_LAB TO ROLE DEVELOPER;

GRANT CREATE SCHEMA ON DATABASE SNOWFLAKE_DM_LAB TO ROLE DEVELOPER;


--- TEARDOWN 

USE ROLE DBA;

DROP DATABASE SNOWFLAKE_DM_LAB;