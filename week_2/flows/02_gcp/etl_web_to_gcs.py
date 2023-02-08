from pathlib import Path

import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket


@task(retries=3)
def fetch(dataset_url: str) -> pd.DataFrame:
    """Read taxi data from web into pandas DataFrame"""
    df = pd.read_csv(
        dataset_url,
        parse_dates=[1, 2],
        dtype={'store_and_fwd_flag': str}
    )
    print(f"columns: {df.dtypes}")
    print(f"rows: {len(df)}")
    return df


@task(log_prints=True)
def write_local(df: pd.DataFrame, color: str, dataset_file: str) -> list[Path]:
    """Write DataFrame out as parquet file"""
    current_script_dir = Path(__file__).parent
    relative_data_path = f'data/{color}/{dataset_file}.parquet.gzip'
    abs_data_path = current_script_dir.joinpath(relative_data_path).resolve()
    abs_data_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(abs_data_path, compression="gzip")
    return abs_data_path, relative_data_path


@task(log_prints=True)
def write_gcs(local_path: Path, gcs_path: Path) -> None:
    """Upload local parquet file to GCS"""
    gcs_block: GcsBucket = GcsBucket.load("de-zoomcamp-gcs")
    gcs_block.upload_from_path(from_path=local_path, to_path=gcs_path)


@flow
def etl_web_to_gcs(color: str, year: int, month: int) -> None:
    """The main ETL function"""
    dataset_file = f'{color}_tripdata_{year}-{month:02}'
    dataset_url = f'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz'

    df = fetch(dataset_url)
    local_path, gcs_path = write_local(df, color, dataset_file)
    write_gcs(local_path, gcs_path)


# if __name__ == '__main__':
#     etl_web_to_gcs()
