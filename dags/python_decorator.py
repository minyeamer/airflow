from airflow import DAG
from airflow.decorators import task
import pendulum

with DAG(
    dag_id="python_decorator",
    schedule="0 0 * * *",
    start_date=pendulum.datetime(2025, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    tags=["example"],
) as dag:
    @task(task_id="python_decorator_task")
    def print_input(__input):
        print(__input)

    python_decorator_task = print_input("@task 데코레이터 실행")