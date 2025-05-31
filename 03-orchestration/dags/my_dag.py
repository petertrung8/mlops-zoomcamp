from airflow.sdk import dag, task, DAG
from airflow.providers.standard.operators.python import PythonOperator
from pendulum import datetime

@dag(
        schedule="@daily",
        start_date=datetime(2025, 1, 1),
        description="This dags does...",
        tags=["team_a", "source_a"],
        max_consecutive_failed_dag_runs=3
)
def my_dag():
    
    @task
    def task_a():
        print("Hello from task A")
    
    @task
    def task_b():
        print("Hello from task B")

    @task
    def task_c():
        print("Hello from task C")
    
    @task
    def task_d():
        print("Hello from task D")

    task_a() >> task_b() >> [task_c(), task_d()]

my_dag()