#!/usr/bin/env python
import argparse
import os
from time import time

import pandas as pd
from sqlalchemy import create_engine


def main(params: argparse.Namespace) -> None:
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url: str = params.url

    if url.endswith('.csv.gz'):
        csv_name = 'output.csv.gz'
    else:
        csv_name = 'output.csv'

    os.system(f'wget {url} -O {csv_name}')

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    # Download the CSV
    df_iter = pd.read_csv(
        csv_name,
        iterator=True,
        chunksize=100000,
        parse_dates=[1, 2],
        dtype={'store_and_fwd_flag': str}
    )  # type: pd.io.parsers.readers.TextFileReader

    df = next(df_iter)
    print('df example:\n')
    print(df.head(3))

    # Create table in PSQL without inserting any row
    df.head(0).to_sql(name=table_name, con=engine, if_exists='replace')
    # Add rows to table in PSQL
    df.to_sql(name=table_name, con=engine, if_exists='append')

    for item in df_iter:
        t_start = time()
        item.to_sql(name=table_name, con=engine, if_exists='append')
        t_end = time()
        print(f'inserted another chunk... took {t_end - t_start:.2f} seconds')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgresql', add_help=True)

    parser.add_argument('--user', help='User name for postgres')
    parser.add_argument('--password', help='Password for postgres')
    parser.add_argument('--host', help='Host for postgres')
    parser.add_argument('--port', help='Port for postgres')
    parser.add_argument('--db', help='Database name for postgres')
    parser.add_argument('--table_name', help='Name of the table where we will write the results to')
    parser.add_argument('--url', help='URL of the csv file')

    args = parser.parse_args()

    main(args)
