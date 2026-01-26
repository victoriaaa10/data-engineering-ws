# src/pipeline/main.py
import click
from src.pipeline.ingest_data import run_ingestion

@click.command()
@click.option('--user', required=True, help='Postgres username')
@click.option('--password', required=True, help='Postgres password')
@click.option('--host', required=True, help='Postgres host')
@click.option('--port', required=True, help='Postgres port')
@click.option('--db', required=True, help='Postgres database name')
@click.option('--table_name', required=True, help='Destination table name')
@click.option('--source_path', required=True, help='Path to the parquet file')
@click.option('--batch_size', default=100000, help='Rows per batch')
def cli(user, password, host, port, db, table_name, source_path, batch_size):
    """
    NYC Taxi Data Pipeline Entry Point
    """
    run_ingestion(
        user=user, 
        password=password, 
        host=host, 
        port=port, 
        db=db, 
        table_name=table_name, 
        source_path=source_path, 
        batch_size=batch_size
    )

if __name__ == '__main__':
    cli()