from datetime import datetime, timedelta
from airflow import DAG # type: ignore
from airflow.contrib.operators.ssh_operator import SSHOperator  # type: ignore
from airflow.operators.trigger_dagrun import TriggerDagRunOperator  # type: ignore

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 5, 27),
    # 'retries': 1,
    # 'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG('Stocks_Producer', default_args=default_args, schedule_interval=None, catchup=False) as dag:

    get_last_cut_date = SSHOperator(
        task_id='run_python_producer',
        ssh_conn_id='ssh_default',  #SSH connection in Airflow
        command='/bin/python3 /home/developer/projects/NAYA_Final_Project/kafka_producer.py'

    )