# src/pipeline/ingest_data.py
import os
import logging
import pandas as pd
from sqlalchemy import create_engine
from tqdm.auto import tqdm

def run_ingestion(user, password, host, port, db, table_name, source_path, batch_size):
    # db conn 
    conn_url = f'postgresql://{user}:{password}@{host}:{port}/{db}'
    engine = create_engine(conn_url)

    # extract and validate 
    if not os.path.exists(source_path):
        logging.error(f"File not found: {source_path}")
        return

    logging.info(f"Reading Parquet file: {source_path}")
    df = pd.read_parquet(source_path)

    # transform | type enforcement 
    cols_to_fix = ['VendorID', 'PULocationID', 'DOLocationID', 'passenger_count', 'payment_type']
    for col in cols_to_fix:
        if col in df.columns:
            df[col] = df[col].astype('Int64')

    # load: schema initialization
    logging.info(f"Initializing table schema: {table_name}")
    df.head(0).to_sql(name=table_name, con=engine, if_exists='replace', index=False)

    # load: batch ingestion
    total_rows = len(df)
    logging.info(f"Total rows to ingest: {total_rows}")

    for i in tqdm(range(0, total_rows, batch_size), desc="Ingesting Batches"):
        batch = df.iloc[i : i + batch_size]
        batch.to_sql(name=table_name, con=engine, if_exists='append', index=False)

    logging.info(f"Successfully ingested {total_rows} rows into {table_name}.")
    engine.dispose()