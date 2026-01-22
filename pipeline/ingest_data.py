
import pandas as pd
import click
from sqlalchemy import create_engine
from tqdm.auto import tqdm
import requests
import io

import pyarrow as pa
import pyarrow.parquet as pq


@click.command()
@click.option('--pg-user', default='root', help='PostgreSQL user')
@click.option('--pg-pass', default='root', help='PostgreSQL password')
@click.option('--pg-host', default='pgdatabase', help='PostgreSQL host')
@click.option('--pg-port', default=5432, type=int, help='PostgreSQL port')
@click.option('--pg-db', default='ny_taxi', help='PostgreSQL database')
@click.option('--year', default=2025, type=int, help='Year of data to ingest')
@click.option('--month', default=11, type=int, help='Month of data to ingest')
@click.option('--target-csv_table', default='taxi_zone_lookup', help='Target table name for CSV data')
@click.option('--target-parquet_table', default='green_tripdata_2025-11', help='Target table name for Parquet data')
@click.option('--chunksize', default=100000, type=int, help='Chunk size for reading data')
def run(pg_user, pg_pass, pg_host, pg_port, pg_db, year, month, target_csv_table, target_parquet_table, chunksize):
    # Build engine
    engine = create_engine(f'postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}')

    # ---- CSV (stream) ----
    csv_url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv"
    df_iter_csv = pd.read_csv(csv_url, iterator=True, chunksize=chunksize)

    first = True
    for df_chunk in tqdm(df_iter_csv, desc="Ingesting CSV (taxi_zone_lookup)"):
        if first:
            df_chunk.head(0).to_sql(name=target_csv_table, con=engine, if_exists='replace', index=False)
            first = False
        df_chunk.to_sql(name=target_csv_table, con=engine, if_exists='append', index=False, method='multi')

    # ---- Parquet (stream via PyArrow) ----
    parquet_url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_{year}-{month:02d}.parquet"
    response = requests.get(parquet_url)
    response.raise_for_status()
    pf = pq.ParquetFile(io.BytesIO(response.content))

    first = True
    for record_batch in tqdm(pf.iter_batches(batch_size=chunksize), desc=f"Ingesting Parquet green_tripdata_{year}-{month:02d}"):
        table = pa.Table.from_batches([record_batch])
        df_chunk = table.to_pandas(types_mapper=pd.ArrowDtype)

        if first:
            df_chunk.head(0).to_sql(name=target_parquet_table, con=engine, if_exists='replace', index=False)
            first = False

        df_chunk.to_sql(name=target_parquet_table, con=engine, if_exists='append', index=False, method='multi')


if __name__ == '__main__':
    run()