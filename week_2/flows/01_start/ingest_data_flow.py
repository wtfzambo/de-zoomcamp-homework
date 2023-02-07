#!/usr/bin/env python
# coding: utf-8
import os
from datetime import timedelta

import pandas as pd
from prefect import flow, task
from prefect.tasks import task_input_hash
from prefect_sqlalchemy import SqlAlchemyConnector


@task(log_prints=True, retries=3, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def extract_data(url: str) -> pd.DataFrame:
    # the backup files are gzipped, and it's important to keep the correct
    # extension for pandas to be able to open the file
    if url.endswith('.csv.gz'):
        csv_name = 'output.csv.gz'
    else:
        csv_name = 'output.csv'

    os.system(f"wget {url} -O {csv_name}")

    # Download the CSV
    df_iter = pd.read_csv(
        csv_name,
        iterator=True,
        chunksize=100000,
        parse_dates=[1, 2],
        dtype={'store_and_fwd_flag': str}
    )
    df = next(df_iter)

    return df


@task(log_prints=True, retries=3)
def transform_data(df: pd.DataFrame) -> pd.DataFrame:
    print(f'pre: missing passenger count: {df["passenger_count"].isin([0]).sum()}')
    df = df[df.passenger_count != 0]
    print(f'post: missing passenger count: {df["passenger_count"].isin([0]).sum()}')
    return df


@task(log_prints=True, retries=3)
def ingest_data(df: pd.DataFrame, table_name: str) -> None:
    connection_block: SqlAlchemyConnector = SqlAlchemyConnector.load("postgres-connector")
    with connection_block.get_connection(begin=False) as engine:
        # Create table in PSQL without inserting any row
        df.head(0).to_sql(name=table_name, con=engine, if_exists='replace')
        # Add rows to table in PSQL
        df.to_sql(name=table_name, con=engine, if_exists='append')

    # for item in df_iter:
    #     t_start = time()
    #     item.to_sql(name=table_name, con=engine, if_exists='append')
    #     t_end = time()
    #     print(f'inserted another chunk... took {t_end - t_start:.2f} seconds')


@flow(name='Subflow', log_prints=True)
def log_subflow(table_name: str) -> None:
    print(f'Logging Subflow for: {table_name}')


@flow(name='Ingest flow')
def main_flow(table_name: str) -> None:
    url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz"

    log_subflow(table_name)
    raw_data = extract_data(url)
    data = transform_data(raw_data)  # type: ignore
    ingest_data(data, table_name)


if __name__ == '__main__':
    main_flow('yellow_taxi_trips')
