from airflow.models import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
from plugins.get_pannes_data import main as get_pannes_data
from plugins.extract_data import main as extract_data
from datetime import timedelta


args = {
    'owner': 'airflow',
    'start_date': days_ago(1)
}

dag = DAG(
    dag_id='get_hydroquebec_data',
    default_args=args,
    schedule_interval=timedelta(minutes=10),
    catchup=False
)

with dag:
    run_task_1 = PythonOperator(
        task_id='run_task_1',
        python_callable=get_pannes_data
    )
    run_task_2 = PythonOperator(
        task_id='run_task_2',
        python_callable=extract_data,
        provide_context=True
    )

    run_task_1 >> run_task_2