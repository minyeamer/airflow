from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.decorators import task
import pendulum

with DAG(
    dag_id="python_template2",
    schedule="0 0 * * *",
    start_date=pendulum.datetime(2025, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    tags=["example"],
) as dag:
    def print_start_end(start_date, end_date, **kwargs):
        print(start_date)
        print(end_date)

    python_task1 = PythonOperator(
        task_id="python_task1",
        python_callable=print_start_end,
        op_kwargs={
            "start_date": "{{ data_interval_start | ds }}",
            "end_date": "{{ data_interval_end | ds }}"
        },
    )

    @task(task_id="python_task2")
    def python_task2(**kwargs):
        for __key in ["ds", "ts", "data_interval_start", "data_interval_end"]:
            if __key in kwargs:
                print(f"{__key}: {kwargs[__key]}")

    python_task1 >> python_task2()