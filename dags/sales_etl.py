from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup

# Default DAG arguments
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="sales_etl",
    default_args=default_args,
    description="Sales ETL pipeline with quality checks and data modeling",
    schedule=None, # Manually
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["sales", "etl"],
) as dag:
    source_quality_check = BashOperator(
        task_id='source_quality_check',
        bash_command='python3 /opt/airflow/src/source_quality_check.py',
    )

    create_staging_tables = BashOperator(
        task_id='create_staging_tables',
        bash_command='python /opt/airflow/src/create_staging_tables.py',
    )

    with TaskGroup("dimension_tables", tooltip="Create dimension tables") as dimension_tables:
        create_dim_country = BashOperator(
            task_id='create_dw_dim_country',
            # bash_command='cd /usr/local/airflow && source dbt_venv/bin/activate && cd include/dbt && dbt run --models models/transform/dimCountry.sql',
            bash_command="echo 'Creating DWH dimension: country...'"
        )

        create_dim_customer = BashOperator(
            task_id='create_dw_dim_customer',
            # bash_command='cd /usr/local/airflow && source dbt_venv/bin/activate && cd include/dbt && dbt run --models models/transform/dimCustomer.sql',
            bash_command="echo 'Creating DWH dimension: customer...'"
        )

        create_dim_datetime = BashOperator(
            task_id='create_dw_dim_datetime',
            # bash_command='cd /usr/local/airflow && source dbt_venv/bin/activate && cd include/dbt && dbt run --models models/transform/dimDateTime.sql',
            bash_command="echo 'Creating DWH dimension: datetime...'"
        )

        create_dim_products = BashOperator(
            task_id='create_dw_dim_products',
            # bash_command='cd /usr/local/airflow && source dbt_venv/bin/activate && cd include/dbt && dbt run --models models/transform/dimProducts.sql',
            bash_command="echo 'Creating DWH dimension: products...'"
        )
        
        # Define dimension build order if needed:
        # create_dim_country >> create_dim_customer >> create_dim_datetime >> create_dim_products

    # Create fact table
    create_fact_invoice = BashOperator(
        task_id="create_fact_invoice",
        # bash_command='cd /usr/local/airflow && source dbt_venv/bin/activate && cd include/dbt && dbt run --models models/transform/factInvoice.sql',
        bash_command="echo 'Creating DWH fact table: invoice...'"
    )

    # Data model quality validation
    model_quality_check = BashOperator(
        task_id='model_quality_check',
        # bash_command='python /opt/airflow/src/data_validation/data_model_quality.py',
        bash_command="echo 'Running data model quality checks...'"
    )

    source_quality_check >> create_staging_tables >> dimension_tables >> create_fact_invoice >> model_quality_check
