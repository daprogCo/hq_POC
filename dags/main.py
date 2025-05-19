from fetch_hq_data import main as fetch_hq_data

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

with dag:
    fetch_hq_data = PythonOperator(
        task_id='fetch_hq_data',
        python_callable=fetch_hq_data
    )

    fetch_hq_data