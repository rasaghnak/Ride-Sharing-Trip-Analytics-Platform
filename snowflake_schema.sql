-- Snowflake DDL & Pipeline Script ─ Ride‑Sharing Trip Analytics Platform
-- Version: 2025‑07‑21
/*============================================================================
  1  DATABASE AND SCHEMA
============================================================================*/
USE ROLE SYSADMIN;
CREATE OR REPLACE DATABASE RIDE_SHARING_DB COMMENT='Ride‑sharing analytics';
CREATE OR REPLACE SCHEMA ANALYTICS COMMENT='Modeled star‑schema';

/*============================================================================
  2  WAREHOUSE
============================================================================*/
CREATE OR REPLACE WAREHOUSE ETL_WH WITH
    WAREHOUSE_SIZE='XSMALL'
    AUTO_SUSPEND = 300
    AUTO_RESUME = TRUE
    COMMENT='Lightweight warehouse for scheduled ETL tasks';

/*============================================================================
  3  FILE FORMATS & STAGES
============================================================================*/
CREATE OR REPLACE FILE FORMAT CSV_GZ_FORMAT
    TYPE = 'CSV'
    COMPRESSION = 'GZIP'
    FIELD_OPTIONALLY_ENCLOSED_BY='"'
    SKIP_HEADER = 1;

CREATE OR REPLACE STAGE STG_RIDES_RAW
    DIRECTORY = (ENABLE = TRUE)
    FILE_FORMAT = CSV_GZ_FORMAT
    COMMENT='Raw ride events (semi‑streaming)';

CREATE OR REPLACE STAGE STG_PAYMENTS_RAW
    DIRECTORY = (ENABLE = TRUE)
    FILE_FORMAT = CSV_GZ_FORMAT
    COMMENT='Raw payment events (semi‑streaming)';

/*============================================================================
  4  RAW (LANDING) TABLES
============================================================================*/
CREATE OR REPLACE TABLE STAGE_RIDES_RAW (
    RIDE_ID            STRING,
    DRIVER_ID          STRING,
    PASSENGER_ID       STRING,
    PICKUP_TS          TIMESTAMP_NTZ,
    DROPOFF_TS         TIMESTAMP_NTZ,
    PICKUP_LAT         FLOAT,
    PICKUP_LON         FLOAT,
    DROPOFF_LAT        FLOAT,
    DROPOFF_LON        FLOAT,
    DISTANCE_MI        FLOAT,
    FARE_AMOUNT        NUMBER(10,2),
    PAYMENT_ID         STRING,
    INGESTION_TS       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE STAGE_PAYMENTS_RAW (
    PAYMENT_ID         STRING,
    RIDE_ID            STRING,
    PAYMENT_TS         TIMESTAMP_NTZ,
    PAYMENT_METHOD     STRING,
    AMOUNT             NUMBER(10,2),
    TIP_AMOUNT         NUMBER(10,2),
    INGESTION_TS       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

/*============================================================================
  5  DIMENSIONS
============================================================================*/
-- DIM_DATE (generated via sequence for 100 years)
CREATE OR REPLACE TABLE DIM_DATE AS
SELECT
    TO_DATE('1970-01-01') + SEQ4() AS DATE_KEY,
    YEAR(DATE_KEY)      AS YEAR_NUM,
    MONTH(DATE_KEY)     AS MONTH_NUM,
    DAY(DATE_KEY)       AS DAY_NUM,
    DAYOFWEEK(DATE_KEY) AS DAY_OF_WEEK,
    QUARTER(DATE_KEY)   AS QUARTER_NUM;

CREATE OR REPLACE TABLE DIM_DRIVER (
    DRIVER_KEY     NUMBER AUTOINCREMENT,
    DRIVER_ID      STRING,
    FIRST_NAME     STRING,
    LAST_NAME      STRING,
    RATING         FLOAT,
    PRIMARY KEY (DRIVER_KEY)
);

CREATE OR REPLACE TABLE DIM_PASSENGER (
    PASSENGER_KEY  NUMBER AUTOINCREMENT,
    PASSENGER_ID   STRING,
    FIRST_NAME     STRING,
    LAST_NAME      STRING,
    PRIMARY KEY (PASSENGER_KEY)
);

CREATE OR REPLACE TABLE DIM_LOCATION (
    LOCATION_KEY   NUMBER AUTOINCREMENT,
    GEO_POINT      GEOGRAPHY,
    GEOHASH_7      STRING,
    BOROUGH        STRING,
    PRIMARY KEY (LOCATION_KEY)
);

CREATE OR REPLACE TABLE DIM_PAYMENT_METHOD (
    METHOD_KEY     NUMBER AUTOINCREMENT,
    PAYMENT_METHOD STRING,
    PRIMARY KEY (METHOD_KEY)
);

/*============================================================================
  6  FACT TABLES (star schema)
============================================================================*/
CREATE OR REPLACE TABLE FACT_RIDES (
    RIDE_KEY            NUMBER AUTOINCREMENT,
    RIDE_ID             STRING,
    DRIVER_KEY          NUMBER,
    PASSENGER_KEY       NUMBER,
    PICKUP_LOCATION_KEY NUMBER,
    DROPOFF_LOCATION_KEY NUMBER,
    PICKUP_TIME         TIMESTAMP_NTZ,
    DROPOFF_TIME        TIMESTAMP_NTZ,
    DISTANCE_MI         FLOAT,
    FARE_AMOUNT         NUMBER(10,2),
    DATE_KEY            DATE,
    PRIMARY KEY (RIDE_KEY)
)
CLUSTER BY(GEOHASH_7);

CREATE OR REPLACE TABLE FACT_PAYMENTS (
    PAYMENT_KEY         NUMBER AUTOINCREMENT,
    PAYMENT_ID          STRING,
    RIDE_KEY            NUMBER,
    PAYMENT_TIME        TIMESTAMP_NTZ,
    METHOD_KEY          NUMBER,
    AMOUNT              NUMBER(10,2),
    TIP_AMOUNT          NUMBER(10,2),
    DATE_KEY            DATE,
    PRIMARY KEY (PAYMENT_KEY)
);

/*============================================================================
  7  STREAMS & TASKS FOR LATE‑ARRIVING EVENTS
============================================================================*/
CREATE OR REPLACE STREAM STG_RIDES_UPDATES ON TABLE STAGE_RIDES_RAW APPEND_ONLY=FALSE;
CREATE OR REPLACE STREAM STG_PAYMENTS_UPDATES ON TABLE STAGE_PAYMENTS_RAW APPEND_ONLY=FALSE;

CREATE OR REPLACE TASK LOAD_RIDES_TASK
    WAREHOUSE=ETL_WH
    SCHEDULE='5 MINUTE'
AS
MERGE INTO FACT_RIDES AS T
USING (
    SELECT
        r.RIDE_ID,
        d.DRIVER_KEY,
        p.PASSENGER_KEY,
        pl.LOCATION_KEY AS PICKUP_LOCATION_KEY,
        dl.LOCATION_KEY AS DROPOFF_LOCATION_KEY,
        r.PICKUP_TS,
        r.DROPOFF_TS,
        r.DISTANCE_MI,
        r.FARE_AMOUNT,
        dd.DATE_KEY
    FROM STG_RIDES_UPDATES r
    LEFT JOIN DIM_DRIVER d      ON d.DRIVER_ID = r.DRIVER_ID
    LEFT JOIN DIM_PASSENGER p   ON p.PASSENGER_ID = r.PASSENGER_ID
    LEFT JOIN DIM_LOCATION pl   ON pl.GEOHASH_7 = GEOHASH(r.PICKUP_LAT, r.PICKUP_LON, 7)
    LEFT JOIN DIM_LOCATION dl   ON dl.GEOHASH_7 = GEOHASH(r.DROPOFF_LAT, r.DROPOFF_LON,7)
    LEFT JOIN DIM_DATE dd       ON dd.DATE_KEY = DATE(r.PICKUP_TS)
) S
ON T.RIDE_ID = S.RIDE_ID
WHEN MATCHED THEN UPDATE SET
    DRIVER_KEY = S.DRIVER_KEY,
    PASSENGER_KEY = S.PASSENGER_KEY,
    PICKUP_LOCATION_KEY = S.PICKUP_LOCATION_KEY,
    DROPOFF_LOCATION_KEY = S.DROPOFF_LOCATION_KEY,
    PICKUP_TIME = S.PICKUP_TS,
    DROPOFF_TIME = S.DROPOFF_TS,
    DISTANCE_MI = S.DISTANCE_MI,
    FARE_AMOUNT = S.FARE_AMOUNT,
    DATE_KEY = S.DATE_KEY
WHEN NOT MATCHED THEN INSERT (
    RIDE_ID, DRIVER_KEY, PASSENGER_KEY, PICKUP_LOCATION_KEY, DROPOFF_LOCATION_KEY,
    PICKUP_TIME, DROPOFF_TIME, DISTANCE_MI, FARE_AMOUNT, DATE_KEY)
VALUES (
    S.RIDE_ID, S.DRIVER_KEY, S.PASSENGER_KEY, S.PICKUP_LOCATION_KEY, S.DROPOFF_LOCATION_KEY,
    S.PICKUP_TS, S.DROPOFF_TS, S.DISTANCE_MI, S.FARE_AMOUNT, S.DATE_KEY);

/* Similar task can be created for FACT_PAYMENTS */

/*============================================================================
  8  SAMPLE ANALYTIC QUERY
============================================================================*/
-- Peak time demand by 15‑minute bucket
SELECT
    DATE_TRUNC('15 MINUTES', PICKUP_TIME) AS INTERVAL_START,
    GEOHASH_7,
    COUNT(*) AS NUM_RIDES
FROM FACT_RIDES
GROUP BY 1,2
ORDER BY NUM_RIDES DESC
LIMIT 100;
