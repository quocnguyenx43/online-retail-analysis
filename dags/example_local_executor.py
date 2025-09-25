from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


# Python function for the PythonOperator
def say_hello():
    print("Hello from Python task in Airflow!")


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
    dag_id="example_local_executor",
    default_args=default_args,
    description="Test DAG for LocalExecutor setup",
    schedule_interval=timedelta(days=1),  # Run once per day
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["example"],
) as dag:

    # Task 1: Print the current date
    print_date = BashOperator(
        task_id="print_date",
        bash_command="date"
    )

    # Task 2: Run a Python function
    hello_task = PythonOperator(
        task_id="say_hello",
        python_callable=say_hello
    )

    # Define task dependencies
    print_date >> hello_task
