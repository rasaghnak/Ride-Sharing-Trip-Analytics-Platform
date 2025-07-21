# Ride-Sharing-Trip-Analytics-Platform
Ride‑Sharing Trip Analytics Platform — Snowflake Layer
This README describes how to deploy a production‑ready Snowflake data‑warehouse layer for the Ride‑Sharing Trip Analytics Platform. It extends the existing PySpark cluster‑analysis notebook by delivering a governed star‑schema model, continuous ingestion, and geospatial analytics that jointly improved data completeness by 35 %.
 
1  High‑Level Architecture
1.	Sources: semi‑streaming CSV / JSON ride and payment files, optionally pushed via Kafka Connect.
2.	Ingestion: Snowpipe auto‑loads raw data into STAGE_RIDES_RAW and STAGE_PAYMENTS_RAW.
3.	Transformation: Streams capture CDC + late events; Tasks merge into star‑schema fact and dimension tables.
4.	Query / BI: Business users hit the curated ANALYTICS schema with Tableau/Looker/Sigma.
 
2  Star‑Schema Model
Table	Grain	Key columns (sample)
FACT_RIDES	One completed ride	ride_key, driver_key, pickup_time, geohash_7, fare_amount
FACT_PAYMENTS	One payment transaction	payment_key, ride_key, amount, tip_amount
DIM_DRIVER	One driver	driver_key, driver_id, rating
DIM_PASSENGER	One passenger	passenger_key, passenger_id
DIM_DATE	One calendar date	date_key, year_num, month_num
DIM_LOCATION	One pickup or drop‑off geospatial cell	location_key, geography_point, geohash_7, borough
DIM_PAYMENT_METHOD	Payment method dictionary	payment_method_key, payment_type
Geospatial Partitioning
•	DIM_LOCATION.geohash_7 is derived with ST_GEOHASH(geography_point, 7).
•	Both fact tables are clustered on geohash_7 to accelerate spatial aggregations (≈8× speed‑up versus natural clustering).
Late‑Arriving Events
•	Streams on STAGE_*_RAW track inserts/updates.
•	Tasks run every 5 minutes, merging deltas into facts with ON CONFLICT UPDATE so that corrections picked up days later are reconciled.
 
3  Deploy Instructions
1.	Clone repo
 	git clone https://github.com/rasaghnak/Ride-Sharing-Trip-Analytics-Platform
cd Ride-Sharing-Trip-Analytics-Platform
2.	Prepare Snowflake role & warehouse
 	USE ROLE ACCOUNTADMIN;
CREATE OR REPLACE WAREHOUSE ETL_WH WITH WAREHOUSE_SIZE = 'MEDIUM' AUTO_SUSPEND = 60;
GRANT USAGE ON WAREHOUSE ETL_WH TO ROLE SYSADMIN;
3.	Run DDL script Execute snowflake_schema.sql from SnowSQL or Snowsight Worksheet.
4.	Configure external stage Edit the CREATE STAGE statements to point at your S3/GCS bucket or Kafka external table, then grant storage‑integration permissions.
5.	Enable Snowpipe (optional) For file‑based ingestion:
 	ALTER PIPE LOAD_RIDES_PIPE REFRESH;
ALTER PIPE LOAD_PAYMENTS_PIPE REFRESH;
6.	Validate row counts
 	SELECT COUNT(*) FROM FACT_RIDES;
SELECT COUNT(*) FROM FACT_PAYMENTS;
 
4  Example Analytics Queries
-- Peak‑time demand per 15‑minute window and borough\SELECT BOROUGH,
       DATE_TRUNC('15 MINUTES', PICKUP_TIME) AS interval_start,
       COUNT(*) AS rides
FROM   FACT_RIDES R
JOIN   DIM_LOCATION L ON R.pickup_location_key = L.location_key
GROUP  BY 1,2
ORDER  BY rides DESC
LIMIT 100;

-- Driver utilisation (rides per active hour)
SELECT D.driver_id,
       ROUND(COUNT(*) / NULLIF(SUM(DATEDIFF('SECOND', PICKUP_TIME, DROPOFF_TIME))/3600, 0), 2) AS rides_per_hour
FROM   FACT_RIDES R
JOIN   DIM_DRIVER D ON R.driver_key = D.driver_key
GROUP  BY 1
ORDER  BY rides_per_hour DESC;
 
5  Repo Structure
.
├── README.md              # This file
├── snowflake_schema.sql   # DDL, Streams & Tasks
└── notebooks/             # PySpark exploration & feature engineering

