from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

# Default arguments for the DAG
default_args = {
    'owner': 'feature_store_team',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Create the DAG
dag = DAG(
    'feature_pipeline',
    default_args=default_args,
    description='Feature engineering pipeline for customer features',
    schedule_interval='0 1 * * *',  # Run daily at 1 AM
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['feature_store', 'ml'],
)

# Task to run Spark transformations
compute_features = SparkSubmitOperator(
    task_id='compute_features',
    application='spark/transformations/feature_transformations.py',
    conn_id='spark_default',
    conf={
        'spark.driver.memory': '4g',
        'spark.executor.memory': '4g',
        'spark.executor.cores': '2',
        'spark.executor.instances': '2'
    },
    dag=dag,
)

# Task to materialize features in the online store
materialize_features = BashOperator(
    task_id='materialize_features',
    bash_command='cd feast/feature_repo && feast materialize-incremental $(date -u +"%Y-%m-%dT%H:%M:%S")',
    dag=dag,
)

# Task to validate feature store
validate_features = BashOperator(
    task_id='validate_features',
    bash_command='cd feast/feature_repo && feast validate-features',
    dag=dag,
)

# Set up task dependencies
compute_features >> materialize_features >> validate_features