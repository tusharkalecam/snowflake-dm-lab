-- Initial setup --

USE DATABASE SNOWFLAKE_DM_LAB;

USE ROLE DEVELOPER; -- We've created this role. You can alter this role to the SYSADMIN role.

USE WAREHOUSE DEVELOPER; -- We've created this role. You can alter this to use the COMPUTE_WH or a warehouse that you create yourself.

-- Creating first db object, a schema, to house all of our integration objects --

CREATE SCHEMA raw;

-- Creating the storage integration to our Azure Data Lake Gen 2 --

/*

Before this, we've created a SERVICE PRINCIPAL in Snowflake for Snowflake to authenticate to Azure.
Find the how-to here: https://docs.snowflake.com/en/user-guide/data-load-azure-config.html

*/

-- We need to use the role of ACCOUNTADMIN to create the integration or role that has been delegated the authorization to create integrations.

USE ROLE ACCOUNTADMIN;

CREATE OR REPLACE STORAGE INTEGRATION az_datalake_storage_integration
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = AZURE
    AZURE_TENANT_ID = '8f9b88a7-3f3e-4be3-aae4-2006d4c42306'
    ENABLED = TRUE
    STORAGE_ALLOWED_LOCATIONS = ('azure://dmlabstorage.blob.core.windows.net/raw')
    COMMENT = 'Storage integration to the shared data lake on Azure';
    
-- DESCRIBE INTEGRATION az_datalake_storage_integration; -- This is where we find information on how to create the service principal.
    
-- We need to give the DEVELOPER role authorization to use the newly created integration.

GRANT USAGE ON INTEGRATION az_datalake_storage_integration TO ROLE DEVELOPER;

-- Switching back to the DEVELOPER ROLE.

USE ROLE DEVELOPER;

-- Creating two file formats to use in my ingestion of data: CSV and JSON.

CREATE OR REPLACE FILE FORMAT raw.csv
    TYPE = CSV
    COMPRESSION = AUTO
    FIELD_DELIMITER = ','
    SKIP_HEADER = 1
    EMPTY_FIELD_AS_NULL = TRUE;

-- Creating the EXTERNAL STAGES for all my directories in the lake, that has been defined by the integration.

CREATE OR REPLACE STAGE extstg_az_stackoverflow_badges
    FILE_FORMAT = ( FORMAT_NAME = 'csv')
    STORAGE_INTEGRATION = az_datalake_storage_integration
    URL = 'azure://dmlabstorage.blob.core.windows.net/raw/stackoverflow/badges';  -- We define the specific path, because this external stage will be used to load data into ONE table.

-- We can now list all the files in that specific path.

LIST @extstg_az_stackoverflow_badges;

-- And select from it, using a special syntax for querying the specific columns in the CSV file.

SELECT t.$1, t.$2, t.$3, t.$4, t.$5, t.$6, t.$7 -- there is no 7th column, so this will just return NULL.  
FROM @extstg_az_stackoverflow_badges t;

-- But, in the resultset we can see, that all strings are enclosed by quotes "". We can alter this in our file format.

CREATE OR REPLACE FILE FORMAT raw.csv
    TYPE = CSV
    COMPRESSION = AUTO
    FIELD_DELIMITER = ','
    SKIP_HEADER = 1
    EMPTY_FIELD_AS_NULL = TRUE
    FIELD_OPTIONALLY_ENCLOSED_BY = '"';
    
-- And query our data again, now without the quotes.

SELECT t.$1, t.$2, t.$3, t.$4, t.$5, t.$6
FROM @extstg_az_stackoverflow_badges t;

-- Now we can create the table where we would like to dump the data into.

CREATE OR REPLACE TRANSIENT TABLE raw.stackoverflow_badges (id NUMBER, userid NUMBER, name VARCHAR, date DATETIME, Class NUMBER, tagbased VARCHAR); -- Creating a transient table, which have limited capability of time-travel and other stuff. Minimizing the storage costs, sinces it a landing table.

COPY INTO raw.stackoverflow_badges
FROM @extstg_az_stackoverflow_badges
PATTERN = '.*[.]csv'; -- Using some pattern recogniztion, to not "ingest" the path itself as we saw earlier.

-- Selecting from the table.

SELECT * FROM raw.stackoverflow_badges;

-- We can also ingest JSON data directly into Snowflake, using the VARIANT datatype.
-- First we create the FILE FORMAT that describes the JSON that is incoming.

/* CREATE JSON FILE FORMAT THROUGH UI - REMEMBER TO BE DEVELOPER ROLE IN RIBBON */

/* For back-up

CREATE OR REPLACE FILE FORMAT "SNOWFLAKE_DM_LAB"."RAW".JSON SET COMPRESSION = 'AUTO' ENABLE_OCTAL = FALSE ALLOW_DUPLICATE = FALSE STRIP_OUTER_ARRAY = TRUE STRIP_NULL_VALUES = FALSE IGNORE_UTF8_ERRORS = FALSE;

*/

-- We can now create the EXTERNAL STAGE for the JSON data we have

CREATE OR REPLACE STAGE extstg_az_stackoverflow_tags
    FILE_FORMAT = ( FORMAT_NAME = 'json')
    STORAGE_INTEGRATION = az_datalake_storage_integration
    URL = 'azure://dmlabstorage.blob.core.windows.net/raw/stackoverflow/tags';  -- We define the specific path, because this external stage will be used to load data into ONE table.

LIST @extstg_az_stackoverflow_tags;

-- Now we create the table to land the data into.

CREATE TRANSIENT TABLE raw.stackoverflow_tags (record_content VARIANT);

-- Copy the data in

COPY INTO raw.stackoverflow_tags
FROM @extstg_az_stackoverflow_tags
PATTERN = '.*[.]json';

-- And selects it.

SELECT * FROM raw.stackoverflow_tags;

-- Now we can do the same for all our other sources 

CREATE OR REPLACE STAGE extstg_az_stackoverflow_comments
    FILE_FORMAT = ( FORMAT_NAME = 'csv')
    STORAGE_INTEGRATION = az_datalake_storage_integration
    URL = 'azure://dmlabstorage.blob.core.windows.net/raw/stackoverflow/comments';
    
    
CREATE OR REPLACE STAGE extstg_az_stackoverflow_posts
    FILE_FORMAT = ( FORMAT_NAME = 'csv')
    STORAGE_INTEGRATION = az_datalake_storage_integration
    URL = 'azure://dmlabstorage.blob.core.windows.net/raw/stackoverflow/posts';
    
CREATE OR REPLACE STAGE extstg_az_stackoverflow_posttags
    FILE_FORMAT = ( FORMAT_NAME = 'csv')
    STORAGE_INTEGRATION = az_datalake_storage_integration
    URL = 'azure://dmlabstorage.blob.core.windows.net/raw/stackoverflow/posttags';
    
CREATE OR REPLACE STAGE extstg_az_stackoverflow_users
    FILE_FORMAT = ( FORMAT_NAME = 'csv')
    STORAGE_INTEGRATION = az_datalake_storage_integration
    URL = 'azure://dmlabstorage.blob.core.windows.net/raw/stackoverflow/users';
    
CREATE OR REPLACE TRANSIENT TABLE raw.stackoverflow_comments    (id NUMBER, postid NUMBER, score NUMBER, text VARCHAR, creationdate DATETIME, userdisplayname VARCHAR, userid VARCHAR, contentlicense VARCHAR);

CREATE OR REPLACE TRANSIENT TABLE raw.stackoverflow_posts       (id NUMBER, posttypeid NUMBER, acceptanswerid VARCHAR, parentid VARCHAR, creationdate DATETIME, deletiondate VARCHAR, score VARCHAR, viewcount VARCHAR, body VARCHAR, owneruserid VARCHAR, ownerdisplayname VARCHAR, lasteditoruserid VARCHAR, lasteditordisplayname VARCHAR, lasteditdate VARCHAR, lastactivitydate DATETIME, title VARCHAR, tags VARCHAR, answercount VARCHAR, commentcount VARCHAR, favoritecount VARCHAR, closeddate VARCHAR, communityowneddate VARCHAR, contentlicense VARCHAR);

CREATE OR REPLACE TRANSIENT TABLE raw.stackoverflow_posttags    (postid NUMBER, tagid NUMBER);

CREATE OR REPLACE TRANSIENT TABLE raw.stackoverflow_users       (id NUMBER, reputation NUMBER, creationdate DATETIME, displayname VARCHAR, lastaccessdate DATETIME, websiteurl VARCHAR, location VARCHAR, aboutme VARCHAR, views NUMBER, upvotes NUMBER, downvotes NUMBER, profileimageurl VARCHAR, emailhash VARCHAR, accountid NUMBER)

COPY INTO raw.stackoverflow_comments
FROM @extstg_az_stackoverflow_comments
PATTERN = '.*[.]csv'

COPY INTO raw.stackoverflow_posts
FROM @extstg_az_stackoverflow_posts
PATTERN = '.*[.]csv';

COPY INTO raw.stackoverflow_posttags
FROM @extstg_az_stackoverflow_posttags
PATTERN = '.*[.]csv';

COPY INTO raw.stackoverflow_users
FROM @extstg_az_stackoverflow_users
PATTERN = '.*[.]csv';

SELECT * FROM raw.stackoverflow_users;
















    