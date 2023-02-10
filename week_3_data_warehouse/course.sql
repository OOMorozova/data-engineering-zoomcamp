-- Creating external table referring to gcs path
CREATE OR REPLACE EXTERNAL TABLE `eco-shift-375819.dezoomcamp.external_yellow_tripdata`
OPTIONS (
  format = 'parquet',
  uris = ['gs://zoom-gcs-camp/data/yellow_tripdata_2019-*.parquet', 'gs://zoom-gcs-camp/data/yellow_tripdata_2020-*.csv']
);


-- Create a partitioned table from external table
--dry_run
CREATE OR REPLACE TABLE `eco-shift-375819.dezoomcamp.yellow_tripdata_patitioned`
PARTITION BY
  DATE(tpep_pickup_datetime) AS
SELECT * FROM `eco-shift-375819.dezoomcamp.external_yellow_tripdata`;

-- Scanning ~106 MB of DATA
SELECT DISTINCT(VendorID)
FROM `eco-shift-375819.dezoomcamp.yellow_tripdata_patitioned`
WHERE DATE(tpep_pickup_datetime) BETWEEN '2019-06-01' AND '2019-06-30';



-- Let's look into the partitons
SELECT table_name, partition_id, total_rows
FROM `dezoomcamp.INFORMATION_SCHEMA.PARTITIONS`
WHERE table_name = 'yellow_tripdata_patitioned'
ORDER BY total_rows DESC;


-- Creating a partition and cluster table
CREATE OR REPLACE TABLE `eco-shift-375819.dezoomcamp.yellow_tripdata_patitioned_clustered`
PARTITION BY DATE(tpep_pickup_datetime)
CLUSTER BY VendorID AS
SELECT * FROM `eco-shift-375819.dezoomcamp..external_yellow_tripdata`;

-- Query scans 864.5 MB
SELECT count(*) as trips
FROM `eco-shift-375819.dezoomcamp.yellow_tripdata_patitioned_clustered`
WHERE DATE(tpep_pickup_datetime) BETWEEN '2019-06-01' AND '2020-12-31'
  AND VendorID=1;
