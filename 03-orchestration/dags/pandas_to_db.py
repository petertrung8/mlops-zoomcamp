from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine


default_args = {
    "start_date": datetime(2023, 1, 1),
}

def generate_df():
    df = pd.DataFrame({
        "name": ["Alice", "Bob", "Charlie"],
        "age": [25, 30, 35],
    })
    return df

def save_to_db(df):
    engine = create_engine('postgresql+psycopg2://airflow:airflow@postgres/airflow')
    df.to_sql('people', con=engine, if_exists='replace', index=False)
    print("Data saved to database successfully.")


with DAG("df_to_postgres",
         default_args=default_args,
        #  schedule_interval=None,
         catchup=False) as dag:

    task_generate = PythonOperator(
        task_id="generate_df",
        python_callable=generate_df
    )

    task_upload = PythonOperator(
        task_id="ingest_to_db",
        python_callable=save_to_db
    )

    task_generate >> task_upload
