from airflow import DAG
from airflow.decorators import task
from airflow.models.taskinstance import TaskInstance
import pendulum

with DAG(
    dag_id="python_xcom1",
    schedule="0 0 * * *",
    start_date=pendulum.datetime(2025, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    tags=["example"],
) as dag:
    @task(task_id="xcom_push_task1")
    def xcom_push_task1(ti: TaskInstance, **kwargs):
        ti.xcom_push(key="key1", value="value1")
        ti.xcom_push(key="key2", value=[1,2,3])

    @task(task_id="xcom_push_task2")
    def xcom_push_task2(ti: TaskInstance, **kwargs):
        ti.xcom_push(key="key1", value="value2")
        ti.xcom_push(key="key2", value=[4,5,6])

    @task(task_id="xcom_pull_task")
    def xcom_pull_task(ti: TaskInstance, **kwargs):
        value1 = ti.xcom_pull(key="key1")
        value2 = ti.xcom_pull(key="key2", task_ids="xcom_push_task1")
        print(value1)
        print(value2)

    xcom_push_task1() >> xcom_push_task2() >> xcom_pull_task()