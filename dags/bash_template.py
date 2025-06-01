from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
import pendulum

with DAG(
    dag_id="bash_template",
    schedule="0 0 * * *",
    start_date=pendulum.datetime(2025, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    tags=["example"],
) as dag:
    bash_task1 = BashOperator(
        task_id="bash_task1",
        bash_command="echo \"End date is {{ data_interval_end }}\"",
    )

    bash_task2 = BashOperator(
        task_id="bash_task2",
        env={
            "START_DATE": "{{ data_interval_start | ds }}",
            "END_DATE": "{{ data_interval_end | ds }}"
        },
        bash_command="echo \"Start date is $START_DATE \" && echo \"End date is $END_DATE\"",
    )

    bash_task1 >> bash_task2