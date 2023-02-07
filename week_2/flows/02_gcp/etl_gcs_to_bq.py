from pathlib import Path

import pandas as pd
from prefect import flow, task
from prefect_gcp import GcpCredentials
from prefect_gcp.cloud_storage import GcsBucket


@task(retries=3)
def extract_from_gcs(color: str, year: int, month: int) -> Path:
    """Download trip data from GCS"""
    data_dir = Path(__file__).parent.joinpath('../../data/').resolve()

    gcs_path = f'data/{color}/{color}_tripdata_{year}-{month:02}.parquet.gzip'
    gcs_block: GcsBucket = GcsBucket.load("de-zoomcamp-gcs")
    gcs_block.get_directory(from_path=gcs_path, local_path=data_dir)
    return Path(f'{data_dir}/{gcs_path}')


@task(log_prints=True)
def transform(path: Path) -> pd.DataFrame:
    """Data cleaning example"""
    df = pd.read_parquet(path)
    print(f'pre: missing passenger count: {df.passenger_count.isna().sum()}')
    df['passenger_count'].fillna(0, inplace=True)
    print(f'post: missing passenger count: {df.passenger_count.isna().sum()}')

    return df


@task
def write_bq(df: pd.DataFrame) -> None:
    """Write DataFrame to BigQuery"""
    creds: GcpCredentials = GcpCredentials.load('de-zoomcamp-gcp-creds')
    df.to_gbq(
        destination_table='trips_data_all.ny_trips',
        project_id='de-zoomcamp-wtfzalgo',
        credentials=creds.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists='append'
    )


@flow(log_prints=True)
def etl_gcs_to_bq():
    """Main ETL flow to load data into BigQuery"""
    color = 'yellow'
    year = '2021'
    month = 1

    path = extract_from_gcs(color, year, month)
    df = transform(path)
    write_bq(df)


if __name__ == '__main__':
    etl_gcs_to_bq()
