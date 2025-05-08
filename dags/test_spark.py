from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
}

with DAG(
    dag_id="test_spark",
    default_args=default_args,
    schedule=None,  # Airflow >= 2.9 uses 'schedule' instead of 'schedule_interval'
    catchup=False,
    tags=["spark", "test"],
) as dag:

    spark_task = SparkSubmitOperator(
        task_id="run_hello_spark",
        application="/opt/airflow/dags/scripts/hello_spark.py",
        conn_id="spark_default",
        spark_binary="/opt/spark/bin/spark-submit",
        verbose=True,
        conf={"spark.master": "spark://spark-master:7077"},
        deploy_mode="client"
    )
