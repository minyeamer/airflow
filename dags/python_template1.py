from airflow.sdk import DAG, task
import pendulum

with DAG(
    dag_id="python_template1",
    schedule="0 0 * * *",
    start_date=pendulum.datetime(2025, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    tags=["example", "template"],
) as dag:
    @task(task_id="show_templates")
    def show_templates(**kwargs):
        from pprint import pprint
        pprint(kwargs)

    show_templates()