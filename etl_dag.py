from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.models import Variable
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    dag_id='dynamic_spark_etl_pipeline',
    default_args=default_args,
    schedule_interval=None, 
    catchup=False,
    tags=['etl', 'spark', 'postgres', 'minio']
) as dag:

    
    table_configs = Variable.get("etl_tables_config", deserialize_json=True, default_var=[])

    for index, config in enumerate(table_configs):
        table_name = config.get("postgres_table")
        s3_path = config.get("s3_path")

        if not table_name or not s3_path:
            continue

       
        BashOperator(
            task_id=f"extract_and_load_{table_name}",
            bash_command=(
                f"python /opt/airflow/files/spark_etl.py "
                f"--postgres_table {table_name} "
                f"--s3_path {s3_path} "
                f"--etl_date {{{{ ds }}}}"
            )
        )