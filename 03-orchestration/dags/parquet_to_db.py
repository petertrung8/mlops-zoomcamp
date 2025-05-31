from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import os
from sqlalchemy import create_engine


default_args = {
    "start_date": datetime(2023, 1, 1),
}

PARQUET_FILE = "/tmp/output.parquet"
DESTINATION_BLOB_NAME = "data/output.parquet"

def generate_parquet():
    df = pd.DataFrame({
        "name": ["Alice", "Bob", "Charlie"],
        "age": [25, 30, 35],
    })
    table = pa.Table.from_pandas(df)
    pq.write_table(table, PARQUET_FILE)

def upload_to_db():
    engine = create_engine('postgresql+psycopg2://airflow:airflow@postgres/airflow')

    df= pd.read_parquet(PARQUET_FILE)
    df.to_sql('people', con=engine, if_exists='replace', index=False)
    print("Data saved to database successfully.")
    # Optional cleanup
    os.remove(PARQUET_FILE)

with DAG("parquet_to_db",
         default_args=default_args,
        #  schedule_interval=None,
         catchup=False) as dag:

    task_generate = PythonOperator(
        task_id="generate_parquet",
        python_callable=generate_parquet
    )

    task_upload = PythonOperator(
        task_id="upload_to_db",
        python_callable=upload_to_db
    )

    task_generate >> task_upload