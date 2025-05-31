from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
import pendulum
import random

with DAG(
    dag_id="python_operator",
    schedule="0 0 * * *",
    start_date=pendulum.datetime(2025, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    tags=["example"],
) as dag:
    def select_country():
        COUNTRIES = [
            "Argentina", "Australia", "Brazil", "Canada", "China", "France", "Germany",
            "India", "Indonesia", "Italy", "Japan", "Mexico", "Russia", "Saudi Arabia",
            "South Africa", "South Korea", "Turkey", "United Kingdom", "United States"
        ]
        print(random.choice(COUNTRIES))

    python_task = PythonOperator(
        task_id="python_task",
        python_callable=select_country,
    )

    python_task