CREATE OR REPLACE EXTERNAL TABLE trips_data_all.fhv_data_external
OPTIONS (
  format = 'CSV',
  uris = ['gs://de_zoomcamp_data_lake_de-zoomcamp-wtfzalgo/data/fhv/fhv_tripdata_*']
);

-- takes a while cuz it's copying all data from GCS to BQ
CREATE OR REPLACE TABLE trips_data_all.fhv_data_native
AS
SELECT * FROM trips_data_all.fhv_data_external;

-- create a partition and cluster table
CREATE OR REPLACE TABLE trips_data_all.fhv_data_native_partitioned_clustered
PARTITION BY
    DATE(pickup_datetime)
CLUSTER BY
    Affiliated_base_number
AS
SELECT * FROM trips_data_all.fhv_data_native;
