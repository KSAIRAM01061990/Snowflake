

 SHOW INTEGRATIONS ;

CREATE OR REPLACE STAGE FLIGHT_DATA
URL = 's3://sairamkanamarlapudi/2020/07/01/'
STORAGE_INTEGRATION = S3_INT
DIRECTORY = (
    ENABLE = true
    AUTO_REFRESH = true
  )
FILE_FORMAT = (TYPE = JSON);

SELECT * FROM DIRECTORY(@FLIGHT_DATA) ;

SELECT parse_json($1) AS VALUE  FROM @FLIGHT_DATA;


CREATE OR REPLACE TABLE FLIGHTDATA
(
FLIGHT_DATA VARIANT
) ;

--INSERT INTO SNOWFLAKE TABLE 

COPY INTO FLIGHTDATA
FROM @FLIGHT_DATA;

SELECT 
FLIGHT_DATA:flights[0]:ACTUAL_ELAPSED_TIME FROM 

FLIGHTDATA;

SELECT *,

VALUE:ACTUAL_ELAPSED_TIME::FLOAT AS ACTUAL_ELAPSED_TIME,
VALUE:DESTINATION.ARR_DELAY:: FLOAT AS ARR_DELAY,

FROM 

FLIGHTDATA AS S ,
LATERAL FLATTEN (input => FLIGHT_DATA:flights ) 


--PARSE_JSON

CREATE OR REPLACE TABLE vartab (n NUMBER(2), v VARIANT);

INSERT INTO vartab
  SELECT column1 AS n, PARSE_JSON(column2) AS v
    FROM VALUES (1, 'null'), 
                (2, null), 
                (3, 'true'),
                (4, '-17'), 
                (5, '123.12'), 
                (6, '1.912e2'),
                (7, '"Om ara pa ca na dhih"  '), 
                (8, '[-1, 12, 289, 2188, false,]'), 
                (9, '{ "x" : "abc", "y" : false, "z": 10} ') 
       AS vals;

    
SELECT * FROM vartab
