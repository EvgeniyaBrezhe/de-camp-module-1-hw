To run the data ingestion with the updated configuration, use the following command:

  docker run -it \
  --network=de-camp-module-1-hw_default \
  taxi_ingest:v001 \
    --pg-user=root \
    --pg-pass=root \
    --pg-host=pgdatabase \
    --pg-port=5432 \
    --pg-db=ny_taxi \
    --target-csv_table=taxi_zone_lookup \
    --target-parquet_table=green_tripdata_2025_11 \
    --year=2025 \
    --month=11 \
    --chunksize=100000