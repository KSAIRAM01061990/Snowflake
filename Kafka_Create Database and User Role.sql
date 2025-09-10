USE ROLE ACCOUNTADMIN;

--Create database and schema
create database if not exists analytics ;
create schema if not exists analytics.kafka_schema;

-- Use a role that can create and manage roles and privileges.
USE ROLE securityadmin;

--create kafka user
CREATE USER IF NOT EXISTS kafka_connector_user_1;

-- Create a Snowflake role with the privileges to work with the connector.
CREATE ROLE kafka_connector_role_1;

-- Grant privileges on the database.
GRANT USAGE ON DATABASE analytics TO ROLE kafka_connector_role_1;


-- Grant privileges on the schema.
GRANT USAGE ON SCHEMA ANALYTICS.kafka_schema TO ROLE kafka_connector_role_1;
GRANT USAGE ,CREATE TABLE ON SCHEMA ANALYTICS.kafka_schema TO ROLE kafka_connector_role_1;
GRANT CREATE STAGE ON SCHEMA ANALYTICS.kafka_schema TO ROLE kafka_connector_role_1;
GRANT CREATE PIPE ON SCHEMA ANALYTICS.kafka_schema TO ROLE kafka_connector_role_1;
GRANT USAGE ON WAREHOUSE COMPUTE_WH TO ROLE kafka_connector_role_1 ;

-- Grant the custom role to an existing user.
GRANT ROLE kafka_connector_role_1 TO USER kafka_connector_user_1;
GRANT ROLE kafka_connector_role_1 TO ROLE SECURITYADMIN;

-- Set the custom role as the default role for the user.
-- If you encounter an 'Insufficient privileges' error, verify the role that has the OWNERSHIP privilege on the user.
ALTER USER kafka_connector_user_1 SET DEFAULT_ROLE = kafka_connector_role_1;



SHOW GRANTS TO ROLE SECURITYADMIN