from pathlib import Path

import requests
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket


@task(log_prints=True, retries=3)
def download_from_web(dataset_file: str, dataset_parent_url: str) -> tuple[Path, Path]:
    current_script_dir = Path(__file__).parent
    relative_file_path = f'data/fhv/{dataset_file}'
    absolute_file_path = current_script_dir.joinpath(relative_file_path).resolve()

    absolute_file_path.parent.mkdir(parents=True, exist_ok=True)

    dataset_url = f'{dataset_parent_url}/{dataset_file}'

    with open(absolute_file_path, 'wb') as file:
        print(f'Downloading dataset from: {dataset_url}')
        res = requests.get(dataset_url)
        res.raise_for_status()
        file.write(res.content)

    return absolute_file_path, relative_file_path


@task(log_prints=True)
def upload_to_gcs(local_dataset_path: Path, gcs_target_path: Path) -> None:
    print(f'Uploading file to GCS: {gcs_target_path}')
    gcs_block: GcsBucket = GcsBucket.load('de-zoomcamp-gcs')
    gcs_block.upload_from_path(from_path=local_dataset_path, to_path=gcs_target_path)
    print('File upload complete')


@flow(log_prints=True)
def sub_flow(year: int, month: int) -> None:
    DATASET_PARENT_URL = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/'
    dataset_file = f'fhv_tripdata_{year}-{month:02}.csv.gz'

    local_dataset_path, gcs_target_path = download_from_web(dataset_file, DATASET_PARENT_URL)
    upload_to_gcs(local_dataset_path, gcs_target_path)


@flow(log_prints=True)
def main_flow(year: int) -> None:
    for month in range(12):
        month = month + 1
        print(f'Downloading data from month: {month:02}')
        sub_flow(year, month)
