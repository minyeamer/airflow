from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from common.common_func import print_now
import pendulum

with DAG(
    dag_id="python_plugins",
    schedule="0 0 * * *",
    start_date=pendulum.datetime(2025, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    tags=["example"],
) as dag:
    python_plugins_task = PythonOperator(
        task_id="python_plugins_task",
        python_callable=print_now,
    )

    python_plugins_task