from airflow import DAG
from airflow.operators.empty import EmptyOperator
from datetime import datetime

with DAG(
    dag_id="dummy_dag",
    start_date=datetime(2025,1,1),
    schedule_interval=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="start")