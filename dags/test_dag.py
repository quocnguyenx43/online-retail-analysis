from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

# Default DAG arguments
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

# Define the DAG
with DAG(
    dag_id="hello_airflow",
    default_args=default_args,
    description="A simple test DAG",
    schedule_interval=None,  # Run once a day
    start_date=None,
    catchup=False,
    tags=["example", "test"],
) as dag:

    # Task 1: print the current date
    print_date = BashOperator(
        task_id="print_date",
        bash_command="date",
    )

    # Task 2: echo a hello message
    say_hello = BashOperator(
        task_id="say_hello",
        bash_command="echo 'Hello from Airflow!'",
    )

    # Task dependencies
    print_date >> say_hello
