from datetime import timedelta
from pathlib import Path
from typing import Literal

import pandas as pd
import requests
from prefect import flow, task
from prefect.filesystems import LocalFileSystem
from prefect.tasks import task_input_hash
from prefect_gcp import GcpCredentials


@task(name='Delete BQ table', log_prints=True)
def delete_bq_table(colors: list[str]) -> None:
    print('Emptying existing table...')
    creds: GcpCredentials = GcpCredentials.load('de-zoomcamp-gcp-creds')

    bq = creds.get_bigquery_client(project='de-zoomcamp-wtfzalgo', location='eu-west-6')
    for color in colors:
        bq.delete_table(table=f'trips_data_all.{color}_trips', not_found_ok=True)


@task(
    name='Write dataset on disk',
    log_prints=True,
    cache_key_fn=task_input_hash,
    cache_expiration=timedelta(days=90)
)
def write_local(url: str, color: str, dataset_file: str) -> Path:
    """Write DataFrame out as parquet file"""
    local_file_system_block: LocalFileSystem = LocalFileSystem.load("de-zoomcamp-data-path")
    base_path = Path(local_file_system_block.basepath).expanduser()

    relative_data_path = f'data/{color}/{dataset_file}.csv.gz'
    abs_data_path = base_path.joinpath(relative_data_path).resolve()
    abs_data_path.parent.mkdir(parents=True, exist_ok=True)

    with open(abs_data_path, 'wb') as file:
        print(f'Downloading dataset from: {url}')
        res = requests.get(url)
        res.raise_for_status()
        file.write(res.content)

    return abs_data_path


@task(name='Write to bigquery', log_prints=True, retries=3)
def read_local_and_write_bq(path: Path, color: str) -> None:
    df = pd.read_csv(
        path,
        parse_dates=[1, 2],
        dtype={'store_and_fwd_flag': str},
        chunksize=500_000
    )
    creds: GcpCredentials = GcpCredentials.load('de-zoomcamp-gcp-creds')

    print('Writing to bigquery')

    for (idx, chunk) in enumerate(df):
        print(f'parsing chunk {idx}')
        print(chunk.head(2))
        chunk.to_gbq(
            destination_table=f'trips_data_all.{color}_trips',
            project_id='de-zoomcamp-wtfzalgo',
            credentials=creds.get_credentials_from_service_account(),
            chunksize=500_000,
            if_exists='append'
        )


@flow(name='Get all months and save local', log_prints=True, persist_result=True)
def get_all_months(color: str, year: int, months: int) -> None:
    paths = []
    for month in range(months):
        month = month + 1
        dataset_file = f'{color}_tripdata_{year}-{month:02}'
        dataset_url = f'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz'
        data_path = write_local(dataset_url, color, dataset_file)
        paths.append(data_path)

    return paths


@flow(name='Get color + year combinations', log_prints=True, persist_result=True)
def write_all_local(colors: list[str], years: list[int], months: int) -> list[Path]:
    color_year_combinations = [{'color': c, 'year': y} for c in colors for y in years]

    local_paths = {}
    for combination in color_year_combinations:
        color = combination['color']
        year = combination['year']
        local_paths = {color: []} | local_paths
        local_paths[color].extend(get_all_months(color, year, months))

    return local_paths


@flow(name='Upload all datasets to BQ', log_prints=True)
def upload_all_to_bq(local_paths: dict[str, list[Path]]):
    for color, paths in local_paths.items():
        for path in paths:
            print(f'df chunked loaded from {path}')
            read_local_and_write_bq(path, color)


@flow(name='Prepare dbt table on BQ', log_prints=True, persist_result=True)
def main_flow(
    colors: list[Literal['yellow', 'green']] = ['yellow', 'green'],
    years: list[Literal[2019, 2020]] = [2019, 2020],
    months: Literal[tuple(range(1, 13))] = 12,
    replace_table: bool = True
) -> None:
    if replace_table:
        delete_bq_table(colors)

    local_paths = write_all_local(colors, years, months)
    upload_all_to_bq(local_paths)
