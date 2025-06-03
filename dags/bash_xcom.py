from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
import pendulum

with DAG(
    dag_id="bash_xcom",
    schedule="0 0 * * *",
    start_date=pendulum.datetime(2025, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    tags=["example"],
) as dag:
    bash_push_task = BashOperator(
        task_id="bash_push_task",
        bash_command="echo START && echo XCOM PUSHED {{ ti.xcom_push(key='bash_pushed', value='bash_message') }} && echo COMPLETE",
    )

    bash_pull_task = BashOperator(
        task_id="bash_pull_task",
        env={
            "PUSHED_VALUE": "{{ ti.xcom_pull(key='bash_pushed', task_ids='bash_push_task') }}",
            "RETURN_VALUE": "{{ ti.xcom_pull(key='return_value', task_ids='bash_push_task') }}"
        },
        bash_command="echo $PUSHED_VALUE && echo $RETURN_VALUE",
    )

    bash_push_task >> bash_pull_task