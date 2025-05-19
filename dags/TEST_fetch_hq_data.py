import requests
from airflow.models import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from datetime import timedelta

HQ_API_URL_TIMESTAMP = "http://pannes.hydroquebec.com/pannes/donnees/v3_0/bisversion.json"
HQ_API_URL_PANNES = "http://pannes.hydroquebec.com/pannes/donnees/v3_0/bismarkers{BIS_VERSION}.json"

def fetch_data(url, error_message):
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        return data
    except requests.RequestException as e:
        print(f"{error_message}: {e}")
        return None

def set_url(url, bis_version):
    return url.replace("{BIS_VERSION}", bis_version)

def main():
    timestamp = fetch_data(HQ_API_URL_TIMESTAMP, "Erreur horodatage")
    if timestamp:
        url = set_url(HQ_API_URL_PANNES, timestamp)
        pannes_data = fetch_data(url, "Erreur pannes")
        if pannes_data:
            return pannes_data
        
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
        python_callable=main
    )

    run_task_1