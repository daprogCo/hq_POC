from fetch_hq_data import main as fetch_hq_data_main
from copy_data_to_psql import main as copy_data_to_psql_main
from end_pannes import main as end_main
from get_meteo24h import main as get_meteo24h_main

from airflow.models import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from datetime import timedelta
    
args = {
    'owner': 'airflow',
    'start_date': days_ago(1)
}

# Ceci est le DAG principal qui orchestre les tâches de récupération des données, de copie dans PostgreSQL, de fin des pannes et de récupération des données météorologiques.
# Il est exécuté toutes les 10 minutes.

# Le DAG est composé de quatre tâches :
# 1. Récupération des données de Hydro-Québec
# 2. Copie des données dans PostgreSQL
# 3. Fin des pannes en cours
# 4. Récupération des données météorologiques pour les pannes terminées

dag = DAG(
    dag_id='main',
    default_args=args,
    schedule_interval=timedelta(minutes=10),
    catchup=False
)

def task_fetch():
    return fetch_hq_data_main()

def task_copy(**context):
    ti = context['ti']
    data = ti.xcom_pull(task_ids='fetch_hq_task')
    if not data:
        print("No data received from fetch_hq_task.")
        return None
    else:
        print(f"Data received: {data}")
    return copy_data_to_psql_main(data)

def task_end(**context):
    ti = context['ti']
    data = ti.xcom_pull(task_ids='copy_data_task')
    if not data:
        print("No data received from copy_data_task.")
        return None
    return end_main(data)

def task_get_meteo24h(**context):
    ti = context['ti']
    data = ti.xcom_pull(task_ids='end_pannes_task')
    if not data:
        print("No data received from end_pannes_task.")
        return None
    return get_meteo24h_main(data)

with dag:
    fetch_hq = PythonOperator(
        task_id='fetch_hq_task',
        python_callable=task_fetch
    )

    copy_to_psql = PythonOperator(
        task_id='copy_data_task',
        python_callable=task_copy
    )

    end_pannes = PythonOperator(
        task_id='end_pannes_task',
        python_callable=task_end
    )

    get_meteo24h = PythonOperator(
        task_id='get_meteo24h_task',
        python_callable=task_get_meteo24h
    )
    
    fetch_hq >> copy_to_psql >> end_pannes >> get_meteo24h
