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
def count_rows(path: Path) -> tuple[pd.DataFrame, int]:
    df = pd.read_parquet(path)
    n_df_rows = len(df)
    print(f'df rows: {len(df)}')
    return df, n_df_rows


@task(log_prints=True)
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
def gcs_to_bq(color: str, year: int, month: int):
    """Main ETL flow to load data into BigQuery"""

    file_path = extract_from_gcs(color, year, month)
    df, n_rows = count_rows(file_path)
    write_bq(df)
    return n_rows


@flow(log_prints=True)
def parent_flow(color: str, year: int, months: list[int]):
    total_processed_rows = 0

    for month in months:
        rows_processed = gcs_to_bq(color, year, month)
        total_processed_rows += rows_processed

    print(f'total processed rows: {total_processed_rows}')


if __name__ == '__main__':
    color = 'yellow'
    year = 2021
    months = [2, 3]
    parent_flow(color, year, months)
