from airflow import DAG
from datetime import datetime
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
}

with DAG(
    dag_id="test_spark",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["spark", "test"],
) as dag:

    spark_task = SparkSubmitOperator(
        task_id="run_hello_spark",
        application="/opt/airflow/dags/scripts/hello_spark.py",  # chemin dans le conteneur
        conn_id="spark_default",
        conf={"spark.master": "spark://spark-master:7077"},
        application_args=[],
        verbose=True
    )
