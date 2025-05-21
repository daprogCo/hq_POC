from fetch_hq_data import main as fetch_hq_data_main
from copy_data_to_psql import main as copy_data_to_psql_main

from airflow.models import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from datetime import timedelta
    
args = {
    'owner': 'airflow',
    'start_date': days_ago(1)
}

dag = DAG(
    dag_id='main',
    default_args=args,
    schedule_interval=timedelta(minutes=10),
    catchup=False
)

def task_fetch(**context):
    return fetch_hq_data_main()

def task_copy(**context):
    ti = context['ti']
    data = ti.xcom_pull(task_ids='fetch_hq_task')
    copy_data_to_psql_main(data)
    

with dag:
    fetch_hq = PythonOperator(
        task_id='fetch_hq_task',
        python_callable=task_fetch
    )

    copy_to_psql = PythonOperator(
        task_id='copy_data_task',
        python_callable=task_copy
    )

    fetch_hq >> copy_to_psql
