from airflow.sdk import DAG, task
from airflow.exceptions import AirflowException
from airflow.providers.standard.operators.bash import BashOperator
import pendulum

with DAG(
    dag_id="trigger_rule1",
    start_date=pendulum.datetime(2025, 1, 1, tz="Asia/Seoul"),
    schedule="0 0 * * *",
    catchup=False,
    tags=["example", "branch"],
) as dag:
    upstream_task1 = BashOperator(
        task_id="upstream_task1",
        bash_command="echo upstream1"
    )

    @task(task_id="upstream_task2")
    def upstream_task2():
        raise AirflowException("upstream2 Exception")

    @task(task_id="upstream_task3")
    def upstream_task3():
        print("정상 처리")

    @task(task_id="downstream_task", trigger_rule="all_done")
    def downstream_task():
        print("정상 처리")

    [upstream_task1, upstream_task2(), upstream_task3()] >> downstream_task()