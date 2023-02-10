-- Creating external table referring to gcs path
CREATE OR REPLACE EXTERNAL TABLE `eco-shift-375819.dezoomcamp.external_fhv_tripdata`
OPTIONS (
  format = 'CSV',
  uris = ['gs://zoom-gcs-camp/data/fhv_tripdata_2019-*.csv.gz']
);

SELECT count(1) FROM `eco-shift-375819.dezoomcamp.external_fhv_tripdata`;

-- Create a  table from external table
--dry_run
CREATE OR REPLACE TABLE `eco-shift-375819.dezoomcamp.fhv_tripdata` AS
SELECT * FROM `eco-shift-375819.dezoomcamp.external_fhv_tripdata`;

--dry_run
select count(distinct(Affiliated_base_number))
from  `eco-shift-375819.dezoomcamp.external_fhv_tripdata`;

--dry_run
select count(distinct(Affiliated_base_number))
from  `eco-shift-375819.dezoomcamp.fhv_tripdata`;


--dry_run
select count(1)
from  `eco-shift-375819.dezoomcamp.fhv_tripdata`
where PUlocationID is null and DOlocationID is null;

CREATE OR REPLACE TABLE `eco-shift-375819.dezoomcamp.fhv_tripdata_patitioned`
PARTITION BY
  DATE(pickup_datetime) AS
SELECT * FROM `eco-shift-375819.dezoomcamp.external_fhv_tripdata`;

select distinct(Affiliated_base_number)
from  `eco-shift-375819.dezoomcamp.fhv_tripdata`
where  DATE(pickup_datetime) BETWEEN '2019-03-01' AND '2019-03-31';

select distinct(Affiliated_base_number)
from  `eco-shift-375819.dezoomcamp.fhv_tripdata_patitioned`
where  DATE(pickup_datetime) BETWEEN '2019-03-01' AND '2019-03-31';
