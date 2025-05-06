from airflow.models import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
from plugins.get_pannes_data import main as get_pannes_data
from datetime import timedelta


args = {
    'owner': 'airflow',
    'start_date': days_ago(1)
}

dag = DAG(
    dag_id='fetch_hq_data',
    default_args=args,
    schedule_interval=timedelta(minutes=10),
    catchup=False
)

with dag:
    run_task_1 = PythonOperator(
        task_id='run_task_1',
        python_callable=get_pannes_data
    )

    run_task_1