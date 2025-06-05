from airflow.sdk import DAG, task
from airflow.models.taskinstance import TaskInstance
import pendulum

with DAG(
    dag_id="python_xcom2",
    schedule="0 0 * * *",
    start_date=pendulum.datetime(2025, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    tags=["example", "xcom"],
) as dag:
    @task(task_id="xcom_return_task")
    def xcom_return_task(**kwargs) -> str:
        return "Success"

    @task(task_id="xcom_pull_task1")
    def xcom_pull_task1(ti: TaskInstance, **kwargs):
        status = ti.xcom_pull(key="return_value", task_ids="xcom_return_task")
        print(f"\"xcom_return_task\" 함수의 리턴 값: \"{status}\"")

    @task(task_id="xcom_pull_task2")
    def xcom_pull_task2(status: str, **kwargs):
        print(f"\"xcom_return_task\" 함수로부터 전달받은 값: \"{status}\"")

    return_value = xcom_return_task()
    return_value >> xcom_pull_task1()
    xcom_pull_task2(return_value)