from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

import mlflow
import pickle
import pandas as pd

from sklearn.feature_extraction import DictVectorizer
from sklearn.metrics import root_mean_squared_error
from sklearn.linear_model import LinearRegression


default_args = {
    "start_date": datetime(2023, 1, 1),
}

#Trying to make these variables global
mlflow.set_tracking_uri("http://03-orchestration-tracking-server-1:5000") #docker container here
mlflow.set_experiment("nyc-taxi-experiment-homework")


def read_dataframe():
    df = pd.read_parquet('https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-03.parquet')
    print("Original size:", df.shape)

    df['duration'] = df.tpep_dropoff_datetime - df.tpep_pickup_datetime
    df.duration = df.duration.dt.total_seconds() / 60

    df = df[(df.duration >= 1) & (df.duration <= 60)]

    categorical = ['PULocationID', 'DOLocationID']
    df[categorical] = df[categorical].astype(str)
    
    print("Preprocessed size:", df.shape)

    # Save dataframe for next tasks
    df.to_parquet('/tmp/df.parquet')


def create_X(df):
    categorical = ['PULocationID', 'DOLocationID']

    dv = DictVectorizer()

    train_dicts = df[categorical].to_dict(orient='records')
    X = dv.fit_transform(train_dicts)

    return X, dv


def prepare_features():
    """Prepare features for training"""
    # Load dataframes
    df = pd.read_parquet('/tmp/df.parquet')
    
    # Create feature matrices
    X, dv = create_X(df)
    
    # Prepare target variables
    target = 'duration'
    y = df[target].values
    
    # Save preprocessed data
    with open('/tmp/X_.pkl', 'wb') as f:
        pickle.dump(X, f)
    with open('/tmp/y.pkl', 'wb') as f:
        pickle.dump(y, f)
    with open('/tmp/dv.pkl', 'wb') as f:
        pickle.dump(dv, f)
    
    print(f"Feature preparation completed")
    print(f"X_train shape: {X.shape}")


def train_lr():
    # Load preprocessed data
    with open('/tmp/X_.pkl', 'rb') as f:
        X_train = pickle.load(f)
    with open('/tmp/y.pkl', 'rb') as f:
        y_train = pickle.load(f)
    with open('/tmp/dv.pkl', 'rb') as f:
        dv = pickle.load(f)
    
    with mlflow.start_run() as run:
        lr = LinearRegression()
        lr.fit(X_train, y_train)

        mlflow.sklearn.log_model(lr, artifact_path="model")

        print(f"intercept: {lr.intercept_}")

with DAG("df_to_postgres",
         default_args=default_args,
        #  schedule_interval=None,
         catchup=False) as dag:

    read_dataframe_task = PythonOperator(
        task_id="read_df",
        python_callable=read_dataframe
    )

    prepare_feat_task = PythonOperator(
        task_id="prepare_features",
        python_callable=prepare_features
    )

    train_lr_task = PythonOperator(
        task_id="train_lr",
        python_callable=train_lr
    )

    read_dataframe_task >> prepare_feat_task >> train_lr_task
